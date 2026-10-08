import { EMPTY_ARRAY, ResponseType, ResponseTypes } from "./protocol/constants"
import { compileSqlTemplate, logError, logNotice, logQuery, safe } from "./utils"
import { ConnectionConfig, ConnectionPartialConfig, StatementMeta, QueryText, Row, StatementName, ConnectionHandlers } from "./types"
import { SocketConnector } from "./protocol/socket-connector"
import { Queue } from "./queue"
import { CollectQuery, StreamQuery, ExecuteQuery, ParseQuery, PostgresQuery } from "./query"
import { sql } from "."
import { Err, Future, Ok, Resolvers } from 'fluent-future'
import { ErrConnectionClosed, ErrConnectionNotConnected, ErrSocketFailed, PostgresError } from "./error"
import { ReadableStreamDefaultController } from "stream/web"
import { authorizeSocket, createSocket, upgradeSocket } from "./protocol/socket-authorization"
import { ConnectionResponseBuffer } from "./protocol/connection-response-reader"
import { NONAME } from "dns"

const logLevels = {
    none: {
        error: false,
        notice: false,
        query: false
    },
    error: {
        error: true,
        notice: false,
        query: false
    },
    notice: {
        error: true,
        notice: true,
        query: false
    },
    query: {
        error: true,
        notice: true,
        query: true
    },
} as const


/**
 * A dedicated connection to PostgreSQL: tagged-template queries, prepared
 * statement caching, transactions with savepoints, and pipelined execution.
 *
 * @example
 * const conn = await Connection.new({ host: 'localhost', user: 'postgres', password: 'postgres', database: 'test' })
 * const users = await conn.query`SELECT * FROM users WHERE id = ${1}`
 * await conn.begin(async tx => tx.query`INSERT INTO users ...`)
 * conn.close()
 */
export class Connection {
    private readonly config: ConnectionConfig

    private _queue = new Queue<PostgresQuery>()
    private _connector: SocketConnector | null = null

    private _connecting: Future<void, PostgresError> | null = null

    private _closing: Resolvers<Future<void, PostgresError>> | null = null
    private _closed = false

    private _parsed = new Map<QueryText, StatementMeta>()
    private _parsing = new Map<QueryText, Future<StatementMeta, PostgresError>>()

    private _stmtCounter = 0
    private _txLevel = 0

    private _emitError(e: PostgresError) { 
        if (logLevels[this.config.logLevel].error) logError(e)
        if (this.config.onError) safe(this.config.onError, e) 
    }
    private _emitNotice(n: PostgresError) { 
        if (logLevels[this.config.logLevel].notice) logNotice(n)
        if (this.config.onNotice) safe(this.config.onNotice, n) 
    }
    private _emitQuery(text: string, args: unknown[]) { 
        if (logLevels[this.config.logLevel].query) logQuery(text, args)
        if (this.config.onQuery) safe(this.config.onQuery, text, args) 
    }
    private _emitNotify(channel: string, payload: string) { 
        const cb = this.config.onNotify
        if (!cb) return

        if (typeof cb === 'function') {
            cb(channel, payload)
        }
        else {
            if (cb[channel]) safe(cb[channel], payload)
        }
    }
    private _emitClose() { 
        if (this.config.onClose) safe(this.config.onClose) 
    }
    private _emitConnect() { 
        if (this.config.onConnect) safe(this.config.onConnect) 
    }

    setHandlers(h: ConnectionHandlers | ((current: ConnectionConfig) => ConnectionHandlers)) {
        const next = typeof h === 'function'
            ? h({...this.config})
            : h

        Object.assign(this.config, next)
        return this
    }


    private _nextStatement() {
        return `s-${this._stmtCounter++}` as StatementName
    }


    constructor(
        config: ConnectionPartialConfig,
    ) {        
        this.config = {
            ...config,
            logLevel: config.logLevel ?? 'error',
            int8toBigint: config.int8toBigint ?? false,
            queryTimeout: config.queryTimeout ?? 30000,
            ssl: config.caPath ? 'require' : (config.ssl ?? 'prefer')
        }
    }


    connect() {
        if (this.isClosed) return Err(ErrConnectionClosed)

        if (this.isConnected) return Ok()

        if (this._connecting) return this._connecting

        this._connecting = createSocket(this.config)
            .andThen(socket => upgradeSocket(socket, this.config))
            .andThen(socket => authorizeSocket(socket, this.config))
            .andThen(socket => {
                const connector = new SocketConnector(
                    socket,
                    (...args) => this._handlePacket(...args),
                    () => {
                        this._cleanup(ErrSocketFailed)
                        this._emitClose()
                    }
                )
                this._connector = connector
                this._connecting = null
                this._emitConnect()
                return Ok()
            })
            .tapErr(() => this._connecting = null)

        return this._connecting
    }


    private _cleanup(reason: PostgresError) {
        this._connector = null
        this._parsed.clear()
        this._parsing.clear()
        this._txLevel = 0

        while (this._queue.hasMore) {
            this._queue.shift.error(reason)
        }
        
        this._tryFinishClosing()
    }


    /**
     * Runs a query, binding template values as `$1, $2, ...`.
     * Parameterized queries are cached as prepared statements.
     *
     * @example
     * const users = await conn.query<User>`SELECT * FROM users WHERE id = ${1}`
     */
    query<T extends Row>(templates: TemplateStringsArray, ...params: any[]) {
        if (this.isClosed) {
            this._emitError(ErrConnectionClosed)
            return Err(ErrConnectionClosed)
        }

        if (!this._connector || this._connector.isClosed) {
            this._emitError(ErrConnectionNotConnected)
            return Err(ErrConnectionNotConnected)
        }

        const { text, args } = compileSqlTemplate(templates, params)

        const resolvers = Future.withResolvers<T[], PostgresError>()

        const parsed = this._parsed.get(text)

        if (parsed) {
            const query = new CollectQuery<T>(
                text, args, parsed, resolvers, this.config.queryTimeout
            )

            const err = this._connector.writeQuery(query)

            if (err) {
                this._emitError(err)
                query.error(err)

                return query.resolvers.future
            }

            this._queue.push(query)

            return query.resolvers.future
        }


        if (!this._parsing.has(text)) {            
            const newMeta = {
                statement: this._nextStatement(), 
                columns: EMPTY_ARRAY, 
                parameters: EMPTY_ARRAY
            }

            const parseQuery = new ParseQuery(
                text, newMeta, Future.withResolvers(), this.config.queryTimeout 
                
            )

            this._connector.writeParse(parseQuery)
            
            this._queue.push(parseQuery)
            
            this._parsing.set(text, parseQuery.resolvers.future)
        }

        const parsing = this._parsing.get(text)!

        return parsing.andThen(meta => {
            const query = new CollectQuery<T>(
                text, args, meta, resolvers, this.config.queryTimeout
            )

            if (!this._connector || this._connector.isClosed) {
                this._emitError(ErrConnectionNotConnected)
                query.error(ErrConnectionNotConnected)

                return query.resolvers.future
            }

            const err = this._connector.writeQuery(query)

            if (err) {
                this._emitError(err)
                query.error(err)
                
                return query.resolvers.future
            }

            this._queue.push(query)

            return query.resolvers.future
        })
    }


    /**
     * Like {@link query}, but for statements that don't return rows (INSERT/UPDATE/DDL/etc).
     *
     * @example
     * await conn.execute`UPDATE users SET name = ${name} WHERE id = ${id}`
     */
    execute(templates: TemplateStringsArray, ...params: any[]) {
        if (this.isClosed) {
            this._emitError(ErrConnectionClosed)
            return Err(ErrConnectionClosed)
        }

        if (!this._connector || this._connector.isClosed) {
            this._emitError(ErrConnectionNotConnected)
            return Err(ErrConnectionNotConnected)
        }

        const {text, args} = compileSqlTemplate(templates, params)

        const resolvers = Future.withResolvers<void, PostgresError>()
        
        const parsed = this._parsed.get(text)

        if (parsed) {
            const query = new ExecuteQuery(
                text, args, parsed, resolvers, this.config.queryTimeout
            )

            const err = this._connector.writeQuery(query)

            if (err) {
                this._emitError(err)
                query.error(err)
                
                return query.resolvers.future
            }

            this._queue.push(query)

            return query.resolvers.future
        }

        if (!this._parsing.has(text)) {
            const newMeta = {
                statement: this._nextStatement(), 
                columns: EMPTY_ARRAY, 
                parameters: EMPTY_ARRAY
            }

            const parseQuery = new ParseQuery(
                text, newMeta, Future.withResolvers(), this.config.queryTimeout 
            )

            this._connector.writeParse(parseQuery)

            this._queue.push(parseQuery)
            
            this._parsing.set(text, parseQuery.resolvers.future)
        }

        const parsing = this._parsing.get(text)!
        
        return parsing.andThen(meta => {
            const query = new ExecuteQuery(
                text, args, meta, resolvers, this.config.queryTimeout
            )

            if (!this._connector || this._connector.isClosed) {
                this._emitError(ErrConnectionNotConnected) 
                query.error(ErrConnectionNotConnected)

                return query.resolvers.future
            }

            const err = this._connector.writeQuery(query)

            if (err) {
                this._emitError(err)
                query.error(err)
                
                return query.resolvers.future
            }

            this._queue.push(query)

            return query.resolvers.future
        })
    }


    /**
     * Streams query results as a `ReadableStream`, without buffering rows in memory.
     * Ideal for large result sets or piping straight into an HTTP response.
     *
     * @example
     * for await (const row of conn.stream<User>`SELECT * FROM orders`) { ... }
     */
    stream<T extends Row>(templates: TemplateStringsArray, ...params: any[]) {
        if (this.isClosed) {
            this._emitError(ErrConnectionClosed)
            throw ErrConnectionClosed
        }

        if (!this._connector || this._connector.isClosed) {
            this._emitError(ErrConnectionNotConnected)
            throw ErrConnectionNotConnected
        }

        const {text, args} = compileSqlTemplate(templates, params)
        
        let controller!: ReadableStreamDefaultController<T>

        const stream = new ReadableStream<T>({
            start: c => {
                controller = c
            }
        })

        const parsed = this._parsed.get(text)

        if (parsed) {
            const query = new StreamQuery<T>(
                text, args, parsed, controller, 
                this.config.queryTimeout
            )

            const err = this._connector.writeQuery(query)

            if (err) {
                this._emitError(err)
                controller.error(err)

                return stream
            }

            this._queue.push(query)

            return stream
        }

        if (!this._parsing.has(text)) {
            const newMeta = {statement: this._nextStatement(), columns: EMPTY_ARRAY, parameters: EMPTY_ARRAY}

            const parseQuery = new ParseQuery(
                text, newMeta, Future.withResolvers(), this.config.queryTimeout 
            )

            this._connector.writeParse(parseQuery)

            this._queue.push(parseQuery)
            
            this._parsing.set(text, parseQuery.resolvers.future)
        }

        const parsing = this._parsing.get(text)!

        parsing
            .tap(meta => {
                const query = new StreamQuery(
                    text, args, meta, controller, 
                    this.config.queryTimeout
                )

                if (!this._connector || this._connector.isClosed) {
                    this._emitError(ErrConnectionNotConnected)
                    controller.error(ErrConnectionNotConnected)

                    return
                }

                const err = this._connector.writeQuery(query)

                if (err) {
                    this._emitError(err)
                    controller.error(err)

                    return
                }

                this._queue.push(query)
            })
            .tapErr(err => {
                controller.error(err)
            })
            .recover()

        return stream
    }


    private _streamWithController<T extends Row>(templates: TemplateStringsArray, params: any[], controller: ReadableStreamDefaultController) {
        if (this.isClosed) {
            this._emitError(ErrConnectionClosed)
            controller.error(ErrConnectionClosed)
            return
        }

        if (!this._connector || this._connector.isClosed) {
            this._emitError(ErrConnectionNotConnected)
            controller.error(ErrConnectionNotConnected)
            return
        }

        const {text, args} = compileSqlTemplate(templates, params)
        
        const parsed = this._parsed.get(text)

        if (parsed) {
            const query = new StreamQuery<T>(
                text, args, parsed, controller, 
                this.config.queryTimeout
            )

            const err = this._connector.writeQuery(query)

            if (err) {
                this._emitError(err)
                controller.error(err)

                return
            }

            this._queue.push(query)

            return 
        }

        if (!this._parsing.has(text)) {
            const newMeta = {statement: this._nextStatement(), columns: EMPTY_ARRAY, parameters: EMPTY_ARRAY}

            const parseQuery = new ParseQuery(
                text, newMeta, Future.withResolvers(), this.config.queryTimeout 
            )

            this._connector.writeParse(parseQuery)

            this._queue.push(parseQuery)
            
            this._parsing.set(text, parseQuery.resolvers.future)
        }

        const parsing = this._parsing.get(text)!

        parsing
            .tap(meta => {
                const query = new StreamQuery(
                    text, args, meta, controller, 
                    this.config.queryTimeout
                )

                if (!this._connector || this._connector.isClosed) {
                    this._emitError(ErrConnectionNotConnected)
                    controller.error(ErrConnectionNotConnected)

                    return
                }

                const err = this._connector.writeQuery(query)

                if (err) {
                    this._emitError(err)
                    controller.error(err)

                    return
                }

                this._queue.push(query)
            })
            .tapErr(err => {
                controller.error(err)
            })
            .recover()
    }


    /**
     * Runs `txCallback` inside `BEGIN`/`COMMIT`, rolling back on error.
     *
     * @example
     * await conn.begin(async tx => {
     *   await tx.query`UPDATE accounts SET balance = balance - 10 WHERE id = 1`
     * })
     */
    begin<T>(txCallback: (db: Connection) => Promise<T>) {
        const currentLevel = this._txLevel

        const cmd = currentLevel
            ? this.execute`savepoint ${sql.ident(`sp_${currentLevel}`)}`
            : this.execute`begin` 

        return cmd
            .andThen(() => {
                this._txLevel++

                return Future.of(() => txCallback(this))
                    .andThen((result) => {                        
                        return currentLevel
                            ? this.execute`release savepoint ${sql.ident(`sp_${currentLevel}`)}`.map(() => result)
                            : this.execute`commit`.map(() => result)
                    })
                    .orElse(err => {                        
                        const rollbackCmd = currentLevel
                            ? this.execute`rollback to savepoint ${sql.ident(`sp_${currentLevel}`)}`
                            : this.execute`rollback`
                        
                        return rollbackCmd.andThen(() => Future.reject(err))
                    })
                    .finally(() => this._txLevel = currentLevel)
            })
    }

    private get _currentQuery() {        
        return this._queue.current
    }


    private _handlePacket(type: ResponseType, length: number, reader: ConnectionResponseBuffer) {
        switch (type) {
            case ResponseTypes.ParseComplete:
            case ResponseTypes.BindComplete:
            case ResponseTypes.CloseComplete: break


            case ResponseTypes.ParameterDescription: {
                const query = this._currentQuery as ParseQuery

                const parameters = reader.readParameterDescription()
                
                query.meta.parameters = parameters                
            } break


            case ResponseTypes.RowDescription: {
                const query = this._currentQuery as ParseQuery

                const columns = reader.readRowDescription()

                query.meta.columns = columns

                this._parsing.delete(query.text)
                this._parsed.set(query.text, query.meta)

                query.complete()
            } break


            case ResponseTypes.NoData: {
                const query = this._currentQuery as ParseQuery

                this._parsing.delete(query.text)
                this._parsed.set(query.text, query.meta)

                query.complete()
            } break


            case ResponseTypes.DataRow: {
                let query = this._currentQuery as ExecuteQuery | StreamQuery<any> | CollectQuery<any>

                if (query instanceof ExecuteQuery) {
                    reader.skipBytes(length)
                    break
                }

                query.push(reader.readDataRow(query.meta.columns, this.config.int8toBigint))
            } break


            case ResponseTypes.CommandComplete: {
                reader.skipBytes(length)

                const query = this._currentQuery as CollectQuery<any> | StreamQuery<any> | ExecuteQuery
                
                query.complete()
                this._emitQuery(query.text, query.args)
            } break

            case ResponseTypes.EmptyQueryResponse: {
                reader.skipBytes(length)
                const query = this._currentQuery
                query.complete()
            } break


            case ResponseTypes.ErrorResponse: {
                const error = reader.readErrorResponse()

                this._emitError(error)

                const query = this._currentQuery

                if (!query) break

                if (query instanceof ParseQuery) this._parsing.delete(query.text)

                query.error(error)
            } break


            case ResponseTypes.ReadyForQuery: {
                reader.skipBytes(length)
                this._queue.next()

                this._tryFinishClosing()
            } break


            case ResponseTypes.Notice: {
                const notice = reader.readErrorResponse()
                
                this._emitNotice(notice)
            } break


            case ResponseTypes.NotificationResponse: {
                const {name, payload} = reader.readNotificationResponse()

                this._emitNotify(name, payload)
            } break


            default: {
                reader.skipBytes(length)
            } break
        }
    }


    /** Whether the connection is alive and usable. */
    get isOpened() {
        return !this._closing && !this._closed
    }


    /** Whether the connection is closed or closing. */
    get isClosed() {
        return this._closed || !!this._closing
    }


    get isConnected() {
        return !!this._connector && !this._connector.isClosed
    }


    private _tryFinishClosing() {
        if (!this._closing) return
        if (this._queue.hasMore) return

        setTimeout(() => {
            if (!this._closing) return

            if (!this._queue.hasMore) {
                this._closing.resolve()
            }
        }, 0)
    }


    /**
     * Closes the connection, awaiting for all pending queries. Not usable afterward.
     */
    close() {
        if (this._closed) return Future.resolve()

        if (this._closing) {
            return this._closing.future
        }

        const closing = Future.withResolvers<void, PostgresError>()
        this._closing = closing

        closing.future.tap(() => {
            this._closing = null
            this._closed = true
            this._connector?.close()
        })

        this._tryFinishClosing()

        return closing.future
    }
}
