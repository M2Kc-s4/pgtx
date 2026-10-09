import { EMPTY_ARRAY, ResponseType, ResponseTypes } from "./protocol/constants"
import { compileSqlTemplate, logError, logNotice, logQuery, safe } from "./utils"
import { ConnectionConfig, ConnectionPartialConfig, StatementMeta, Row, ConnectionEvent, ErrorHandler, NoticeHandler, QueryHandler, NotifyHandler, CloseHandler, Handlers } from "./types"
import { SocketConnector } from "./protocol/socket-connector"
import { Queue } from "./queue"
import { CollectQuery, StreamQuery, ExecuteQuery, ParseQuery, PostgresQuery } from "./query"
import { sql } from "."
import { Err, Future, Ok, Resolvers } from 'fluent-future'
import { ErrConnectionClosed, ErrSocketFailed, PostgresError } from "./error"
import { ReadableStreamDefaultController } from "stream/web"
import { authorizeSocket, createSocket, upgradeSocket } from "./protocol/socket-authorization"
import { ConnectionResponseBuffer } from "./protocol/connection-response-reader"
import { Socket } from "net"

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
 * A single-use PostgreSQL connection over one socket. It never reconnects:
 * once the socket is gone the object is dead, so open a new one with
 * `Connection.connect()` (`Pool` and `Listener` do that for you).
 *
 * @example
 * const conn = await Connection.connect({ host: 'localhost', port: 5432, user: 'postgres', password: 'postgres', database: 'test' })
 * const users = await conn.query`SELECT * FROM users WHERE id = ${1}`
 * await conn.close()
 */
export class Connection {
    private readonly config: ConnectionConfig

    private _queue = new Queue<PostgresQuery>()
    private _connector: SocketConnector

    private _closing: Resolvers<Future<void, PostgresError>> | null = null

    private _parsed = new Map<string, StatementMeta>()
    private _parsing = new Map<string, Future<StatementMeta, PostgresError>>()

    private _stmtCounter = 0
    private _txLevel = 0

    private _handlers = {
        error: new Set<ErrorHandler>(),
        notice: new Set<NoticeHandler>(),
        query: new Set<QueryHandler>(),
        notify: new Set<NotifyHandler>(),
        close: new Set<CloseHandler>()
    } as const

    private _emitError(e: PostgresError) { 
        for (const handler of this._handlers.error) safe(handler, e) 
    }

    private _emitNotice(n: PostgresError) { 
        for (const h of this._handlers.notice) safe(h, n)
    }

    private _emitQuery(text: string, args: unknown[]) { 
        for (const h of this._handlers.query) safe(h, text, args)
    }

    private _emitNotify(channel: string, payload: string) {
        for (const h of this._handlers.notify) safe(h, channel, payload)
    }

    private _emitClose() {
        for (const h of this._handlers.close) safe(h)
    }


    /**
     * Subscribes to a connection event: `error`, `notice`, `query`, `notify` (channel, payload)
     * or `close` (fires once). Handlers are dropped when the connection closes.
     *
     * @example
     * conn.on('error', err => metrics.inc('pg_errors'))
     */
    on(name: 'error', cb: ErrorHandler): void
    on(name: 'notice', cb: NoticeHandler): void
    on(name: 'query', cb: QueryHandler): void
    on(name: 'notify', cb: NotifyHandler): void
    on(name: 'close', cb: CloseHandler): void
    on<K extends ConnectionEvent>(name: K, cb: Handlers[K]): void {
        this._handlers[name].add(cb as any)
    }

    /** Removes a handler added with `on`. */
    off(name: 'error', cb: ErrorHandler): void
    off(name: 'notice', cb: NoticeHandler): void
    off(name: 'query', cb: QueryHandler): void
    off(name: 'notify', cb: NotifyHandler): void
    off(name: 'close', cb: CloseHandler): void
    off<K extends ConnectionEvent>(name: K, cb: Handlers[K]): void {
        this._handlers[name].delete(cb as any)
    }


    private _nextStatement() {
        return `s-${this._stmtCounter++}`
    }


    private constructor(
        socket: Socket,
        config: ConnectionPartialConfig,
    ) {       
        this.config = {
            ...config,
            logLevel: config.logLevel ?? 'error',
            int8toBigint: config.int8toBigint ?? false,
            queryTimeout: config.queryTimeout ?? 30000,
            ssl: config.caPath ? 'require' : (config.ssl ?? 'prefer')
        }

        const log = logLevels[this.config.logLevel]

        if (log.error) this.on('error', logError)
        if (log.notice) this.on("notice", logNotice)
        if (log.query) this.on('query', logQuery)

        this._connector = new SocketConnector(socket,
            (...a) => this._handlePacket(...a),
            () => {
                while (this._queue.hasMore) this._queue.shift.error(ErrSocketFailed)
                if (!this._closing) this._emitError(ErrSocketFailed)

                this._parsed.clear(); this._parsing.clear()

                this._closing?.resolve()

                this._emitClose()
                
                for (const set of Object.values(this._handlers)) set.clear()
            }
        )
    }


    /** Opens a socket, upgrades it to TLS if needed, authorizes, and resolves with a ready connection. */
    static connect(config: ConnectionPartialConfig) {
        return createSocket(config)
            .andThen(s => upgradeSocket(s, config))
            .andThen(s => authorizeSocket(s, config))
            .andThen(s => Ok(new Connection(s, config)))
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

            if (this._connector.isClosed) {
                this._emitError(ErrConnectionClosed)
                query.error(ErrConnectionClosed)

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

            if (this._connector.isClosed) {
                this._emitError(ErrConnectionClosed) 
                query.error(ErrConnectionClosed)

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

                if (this._connector.isClosed) {
                    this._emitError(ErrConnectionClosed)
                    controller.error(ErrConnectionClosed)

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


    /** @internal Used by `Pool.stream` to run a stream on an already created controller. */
    private _streamWithController<T extends Row>(templates: TemplateStringsArray, params: any[], controller: ReadableStreamDefaultController) {
        if (this.isClosed) {
            this._emitError(ErrConnectionClosed)
            controller.error(ErrConnectionClosed)
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

                if (this._connector.isClosed) {
                    this._emitError(ErrConnectionClosed)
                    controller.error(ErrConnectionClosed)

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


    private _handlePacket(type: ResponseType, length: number, reader: ConnectionResponseBuffer) {
        switch (type) {
            case ResponseTypes.ParseComplete:
            case ResponseTypes.BindComplete:
            case ResponseTypes.CloseComplete: break


            case ResponseTypes.ParameterDescription: {
                const query = this._queue.current as ParseQuery

                const parameters = reader.readParameterDescription()
                
                query.meta.parameters = parameters                
            } break


            case ResponseTypes.RowDescription: {
                const query = this._queue.current as ParseQuery

                const columns = reader.readRowDescription()

                query.meta.columns = columns

                this._parsing.delete(query.text)
                this._parsed.set(query.text, query.meta)

                query.complete()
            } break


            case ResponseTypes.NoData: {
                const query = this._queue.current as ParseQuery

                this._parsing.delete(query.text)
                this._parsed.set(query.text, query.meta)

                query.complete()
            } break


            case ResponseTypes.DataRow: {
                let query = this._queue.current as ExecuteQuery | StreamQuery<any> | CollectQuery<any>

                if (query instanceof ExecuteQuery) {
                    reader.skipBytes(length)
                    break
                }

                query.push(reader.readDataRow(query.meta.columns, this.config.int8toBigint))
            } break


            case ResponseTypes.CommandComplete: {
                reader.skipBytes(length)

                const query = this._queue.current as CollectQuery<any> | StreamQuery<any> | ExecuteQuery
                
                query.complete()
                this._emitQuery(query.text, query.args)
            } break

            case ResponseTypes.EmptyQueryResponse: {
                reader.skipBytes(length)
                const query = this._queue.current
                query.complete()
            } break


            case ResponseTypes.ErrorResponse: {
                const error = reader.readErrorResponse()

                this._emitError(error)

                const query = this._queue.current

                if (!query) break

                if (query instanceof ParseQuery) this._parsing.delete(query.text)

                query.error(error)
            } break


            case ResponseTypes.ReadyForQuery: {
                reader.skipBytes(length)
                this._queue.next()

                if (this._closing) this._tryCloseConnector()
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
        return !this._closing && !this._connector.isClosed
    }


    /** Whether the connection is closed or closing. */
    get isClosed() {
        return this._connector.isClosed || !!this._closing
    }


    private _tryCloseConnector() {
        if (this._queue.hasMore) return

        setTimeout(() => {
            if (!this._closing) return

            if (!this._queue.hasMore) {
                this._connector.close()
            }
        }, 0)
    }


    /**
     * Closes the connection, awaiting for all pending queries. Not usable afterward.
     */
    close() {
        if (this._connector.isClosed) return Future.resolve()

        if (this._closing) return this._closing.future

        const resolvers = Future.withResolvers<void, PostgresError>()
        this._closing = resolvers

        resolvers.future.finally(() => this._closing = null).recover()

        this._tryCloseConnector()

        return resolvers.future
    }
}
