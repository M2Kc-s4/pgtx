import { Socket } from "net"
import { EMPTY_ARRAY, ResponseType, ResponseTypes } from "./protocol/constants"
import { compileSqlTemplate, logError, logNotice, logQuery } from "./utils"
import { ChannelName, ConnectionConfig, ConnectionPartialConfig, StatementMeta, QueryText, Row, StatementName } from "./types"
import { SocketConnector } from "./protocol/socket-connector"
import { Queue } from "./queue"
import { CollectQuery, StreamQuery, ExecuteQuery, ParseQuery, PostgresQuery } from "./query"
import { sql } from "."
import { Future, Ok, Resolvers } from 'fluent-future'
import { ErrConnectionClosed, ErrConnectionReconnecting, PostgresError } from "./error"
import { ReadableStreamDefaultController } from "stream/web"
import { authorizeSocket, createSocket, upgradeSocket } from "./protocol/socket-authorization"
import { ConnectionResponseBuffer } from "./protocol/connection-response-reader"



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

    private _closing: Resolvers<Future<void, PostgresError>> | null = null
    private _closed = false
    private _reconnecting: Future<void, PostgresError> | null = null

    private _connector: SocketConnector

    private _parsed = new Map<QueryText, StatementMeta>()
    private _parsing = new Map<QueryText, Future<StatementMeta, PostgresError>>()

    private _listeningCallbacks = new Map<ChannelName, Set<(payload: string) => void>>()
    private _stmtCounter = 0
    private _txLevel = 0

    private _nextStatement() {
        return `s-${this._stmtCounter++}` as StatementName
    }


    private constructor(
        socket: Socket,
        config: ConnectionConfig,
    ) {        
        this.config = config
        this._connector = new SocketConnector(
            config, socket, 
            this._handlePacket.bind(this),
            () => this._reconnect()
        )
    }


    /**
     * Opens a new connection and authenticates.
     * @throws {PostgresError} if authentication fails or the connection can't be established
     */
    static new(config: ConnectionPartialConfig) {
        const conf: ConnectionConfig = {
            ...config,
            logLevel: config.logLevel || 'error',
            int8toBigint: config.int8toBigint || false,
            queryTimeout: config.queryTimeout || 30000,
            syncSсhedule: config.syncSсhedule || 'Immediate',
            ssl: config.caPath ? 'require' : (config.ssl || 'prefer')
        }


        return createSocket(conf)
            .andThen(socket => upgradeSocket(socket, conf))
            .andThen(socket => authorizeSocket(socket, conf))
            .andThen(socket => Ok(new Connection(socket, conf)))
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
            logError(ErrConnectionClosed, this.config.logLevel)
            return Future.reject(ErrConnectionClosed)
        }

        const {text, args} = compileSqlTemplate(templates, params)

        const resolvers = Future.withResolvers<T[], PostgresError>()

        if (this._reconnecting) {
            return this._reconnecting
                .tapErr(err => logError(err, this.config.logLevel))
                .andThen(() => this._performQuery<T>(text, args, resolvers))
        }
        
        return this._performQuery<T>(text, args, resolvers)
    }


    private _performQuery<T extends Row>(text: QueryText, args: unknown[], resolvers: Resolvers<Future<T[], PostgresError>>): Future<T[], PostgresError> {
        const parsed = this._parsed.get(text)
        if (parsed) {
            const query = new CollectQuery<T>(
                text, args, parsed, resolvers, this.config.queryTimeout
            )

            const err = this._connector.writeQuery(query)

            if (err) {
                query.error(err)
                logError(err, this.config.logLevel)

                return query.resolvers.future
            }

            this._queue.push(query)
            logQuery(query, this.config.logLevel)

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
            const err = this._connector.writeQuery(query)

            if (err) {
                query.error(err)
                logError(err, this.config.logLevel)
                
                return query.resolvers.future
            }

            this._queue.push(query)
            logQuery(query, this.config.logLevel)

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
            logError(ErrConnectionClosed, this.config.logLevel)
            return Future.reject(ErrConnectionClosed)
        }

        const {text, args} = compileSqlTemplate(templates, params)

        const resolvers = Future.withResolvers<void, PostgresError>()
            
        if (this._reconnecting) {
            return this._reconnecting
                .tapErr(err => logError(err, this.config.logLevel))
                .andThen(() => this._performExecute(text, args, resolvers))
        }

        return this._performExecute(text, args, resolvers)
    }


    private _performExecute(text: QueryText, args: unknown[], resolvers: Resolvers<Future<void, PostgresError>>): Future<void, PostgresError> {
        const parsed = this._parsed.get(text)

        if (parsed) {
            const query = new ExecuteQuery(
                text, args, parsed, resolvers, this.config.queryTimeout
            )

            const err = this._connector.writeQuery(query)

            if (err) {
                query.error(err)
                logError(err, this.config.logLevel)
                
                return query.resolvers.future
            }

            this._queue.push(query)
            logQuery(query, this.config.logLevel)

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

            const err = this._connector.writeQuery(query)

            if (err) {
                query.error(err)
                logError(err, this.config.logLevel)
                
                return query.resolvers.future
            }

            this._queue.push(query)
            logQuery(query, this.config.logLevel)

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
            logError(ErrConnectionClosed, this.config.logLevel)
            throw ErrConnectionClosed
        }

        const {text, args} = compileSqlTemplate(templates, params)
        
        let controller!: ReadableStreamDefaultController<T>

        const stream = new ReadableStream<T>({
            start: c => {
                controller = c
            }
        })

        if (this._reconnecting) {
            this._reconnecting
                .tap(() => this._performStream<T>(text, args, controller))
                .tapErr(err => {
                    logError(err, this.config.logLevel) 
                    controller.error(err)
                })
                .recover()

            return stream
        }

        this._performStream<T>(text, args, controller)

        return stream
    }
    

    private _performStream<T extends Row>(text: QueryText, args: unknown[], controller: ReadableStreamDefaultController<T>) {
        const parsed = this._parsed.get(text)
        if (parsed) {
            const query = new StreamQuery<T>(
                text, args, parsed, controller, 
                this.config.queryTimeout
            )

            const err = this._connector.writeQuery(query)

            if (err) {
                controller.error(err)
                logError(err, this.config.logLevel)

                return 
            }

            this._queue.push(query)
            logQuery(query, this.config.logLevel)

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

                const err = this._connector.writeQuery(query)

                if (err) {
                    controller.error(err)
                    logError(err, this.config.logLevel)

                    return
                }

                
                this._queue.push(query)
                logQuery(query, this.config.logLevel)
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
            ? this.execute`savepoint ${sql.ident(`sp_\${currentLevel}`)}`
            : this.execute`begin` 

        return cmd
            .andThen(() => {
                this._txLevel++

                return Future.of(() => txCallback(this))
                    .andThen((result) => {
                        this._txLevel--
                        
                        return currentLevel
                            ? this.execute`release savepoint ${sql.ident(`sp_\${currentLevel}`)}`.map(() => result)
                            : this.execute`commit`.map(() => result)
                    })
                    .orElse(err => {
                        this._txLevel--
                        
                        const rollbackCmd = currentLevel
                            ? this.execute`rollback to savepoint ${sql.ident(`sp_\${currentLevel}`)}`
                            : this.execute`rollback`
                        
                        return rollbackCmd.andThen(() => Future.reject(err))
                    })
            })
    }


    /** Sends a `pg_notify` message on `channelName` (payload ≤ 8000 bytes). */
    notify(channelName: string, payload: string = "") {
        return this.execute`select pg_notify(${channelName}, ${payload})`
    }


    /** Subscribes `callback` to `channelName`, issuing `LISTEN` on first subscription. */
    listen(channelName: string, callback: (payload: string) => void) {
        return this.execute`listen ${sql.ident(channelName)};`
            .tap(() => {
                if (!this._listeningCallbacks.has(channelName as ChannelName)) {
                    this._listeningCallbacks.set(channelName as ChannelName, new Set())
                }
                
                this._listeningCallbacks.get(channelName as ChannelName)!.add(callback)
            }) 
    }


    /** Unsubscribes `callback`, issuing `UNLISTEN` once no callbacks remain. */
    unlisten(channelName: string, callback: (payload: string) => void): Future<void, PostgresError> {
        if (!this._listeningCallbacks.has(channelName as ChannelName)) {
            return Ok()
        }

        const callbackSet = this._listeningCallbacks.get(channelName as ChannelName)!

        callbackSet.delete(callback)

        if (callbackSet.size === 0) {
            this._listeningCallbacks.delete(channelName as ChannelName)
            return this.execute`unlisten ${sql.ident(channelName)};`
        }
        
        return Ok()
    }
    

    private _reconnect() {
        if (this.isClosed) return
        if (this._reconnecting) return

        this._reconnecting = this._performReconnect()
            .tap(() => this._reconnecting = null)
            .andThen(() => this._restoreSubscriptions())
            .tapErr(() => {
                this._reconnecting = null
                this._reconnect()
            })
            .recover()
    }


    private _performReconnect() {
        this._connector.destroy()
        this._parsed.clear()
        this._parsing.clear()

        while (this._queue.hasMore) {
            this._queue.shift.error(ErrConnectionReconnecting)
        }
        
        return createSocket(this.config)
            .andThen(socket => upgradeSocket(socket, this.config))
            .andThen(socket => authorizeSocket(socket, this.config))
            .andThen(socket => {
                const connector = new SocketConnector(
                    this.config, socket, 
                    this._handlePacket.bind(this),
                    () => this._reconnect()
                )

                this._connector = connector

                return Ok()
            })           
    }

    
    private _restoreSubscriptions() {
        if (this._listeningCallbacks.size === 0) return Future.resolve()

        const futures = Array.from(this._listeningCallbacks.keys()).map(channel => {
            return this.execute`LISTEN ${sql.ident(channel)};`
        })

        return Future.all(futures).map(() => {})
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


            case ResponseTypes.ComandComplete: {
                reader.skipBytes(length)

                const query = this._currentQuery
                
                query.complete()
            } break


            case ResponseTypes.ErrorResponse: {
                const error = reader.readErrorResponse()

                logError(error, this.config.logLevel)

                const query = this._currentQuery

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
                
                logNotice(notice, this.config.logLevel)
            } break


            case ResponseTypes.NotificationResponse: {
                const {name, payload} = reader.readNotificationResponse()

                const callbackSet = this._listeningCallbacks.get(name)

                if (!callbackSet) break

                callbackSet.forEach(cb => cb(payload))
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
            this._connector.destroy()
        })

        this._tryFinishClosing()

        return closing.future
    }
}
