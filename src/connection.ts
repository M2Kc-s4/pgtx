import { Socket } from "net"
import { DescribeType, EMPTY_ARRAY, INT4Length, ResponseType, ResponseTypes } from "./protocol/constants"
import { compileSqlTemplate } from "./utils"
import { ChannelName, ConnectionConfig, ConnectionPartialConfig, StatementMeta, QueryText, Resolvers, Row, StatementName } from "./types"
import { Transaction } from "./transaction"
import { SocketConnector } from "./protocol/socket-connector"
import { Queue } from "./queue"
import { CollectQuery, StreamQuery, ExecuteQuery, Query, ParseQuery, PostgresQuery } from "./query"
import { EmptyClause, sql } from "."
import { Begin, Future, Ok } from 'fluent-future'
import { ErrConnectionClosed, ErrConnectionReconnecting, PostgresError } from "./error"
import { ReadableStreamDefaultController } from "stream/web"
import { nextTick } from "process"
import { authorizeSocket, createSocket, upgradeSocket } from "./protocol/socket-authorization"
import { ConnectionResponseBuffer } from "./protocol/connection-response-reader"
import { ConnectionRequestBuffer } from "./protocol/connection-request-writer"
import { error } from "console"


const shedule = {
    Immediate: setImmediate,
    afterMicrotask: setTimeout,
    beforeMicrotask: nextTick
}


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

    private _writer = ConnectionRequestBuffer.new(65536)
    private _sheduled = false
    private _queue = new Queue<PostgresQuery>()

    private _closing: Resolvers<Future<void, PostgresError>> | null = null
    private _closed = false
    private _reconnecting: Future<void, PostgresError> | null = null

    private _socket: SocketConnector

    private _parsed = new Map<QueryText, StatementMeta>()
    private _parsing = new Map<QueryText, Future<StatementMeta, PostgresError>>()

    private _listeningCallbacks = new Map<ChannelName, Set<(payload: string) => void>>()
    private _stmtCounter = 0

    private _nextStatement() {
        return `s-${this._stmtCounter++}` as StatementName
    }


    private _logError(error: PostgresError) {
        if (this.config.logLevel === 'error' || this.config.logLevel === 'notice' || this.config.logLevel === 'query') { 
            console.log( 
                `\n\x1b[31m┌─ ERROR ────────────────────────────────────────\x1b[0m\n` 
                + `\x1b[31m│\x1b[0m ${error}\n` 
                + `\x1b[31m└────────────────────────────────────────────────\x1b[0m\n` 
            ) 
        }
    }


    private _logQuery(text: QueryText, args: unknown[]) {
        if (this.config.logLevel === 'query') { 
            console.log(
                `\n\x1b[36m┌─ QUERY ─────────────────────────────────────────\x1b[0m\n`
                + `\x1b[36m│\x1b[0m ${text}\n` 
                + `${args.length !== 0 ? `\x1b[36m│\x1b[0m \x1b[90mArguments:\x1b[0m [${args}]\n` : ''}` 
                + `\x1b[36m└────────────────────────────────────────────────\x1b[0m` 
            ) 
        }
    }


    private _logNotice(notice: PostgresError) {
        if (this.config.logLevel === 'notice' || this.config.logLevel === 'query') { 
            console.log( 
                `\n\x1b[33m┌─ NOTICE ───────────────────────────────────────\x1b[0m\n` 
                + `\x1b[33m│\x1b[0m ${notice}\n` 
                + `\x1b[33m└────────────────────────────────────────────────\x1b[0m\n` 
            ) 
        }
    }


    private _registerSсhedule() {
        if (!this._sheduled) {
            this._sheduled = true
            this._writer.clear()
            shedule[this.config.syncSсhedule](() => this._sсhedule())
        }
    }

    private _sсhedule() {
        if (this._reconnecting) return

        this._writer.hasMore && this._socket.write(this._writer)
        this._writer.clear()
        this._sheduled = false
    }

    
    private _registerQuery(query: PostgresQuery): PostgresError | null {
        this._registerSсhedule()

        if (query instanceof ParseQuery) {
            this._writer
                .writeParse(query.meta.statement, query.text)
                .writeDescribe(DescribeType.Statement, query.meta.statement)
                .writeSync()
                
            this._queue.push(query)

            return null
        }

        const err = this._writer
            .writeBind("", query.meta, query.args)
        
        if (err) return err

        this._writer
            .writeExecute("")
            .writeSync()

        this._queue.push(query)
        
        return null
    }


    private constructor(
        socket: Socket,
        config: ConnectionConfig,
    ) {
        this.config = config
        this._socket = new SocketConnector(socket, 
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
        if (this.isClosed) return Future.reject(ErrConnectionClosed)

        const {text, args} = compileSqlTemplate(templates, params)

        const resolvers = Future.withResolvers<T[], PostgresError>()

        if (this._reconnecting) return this._reconnecting.andThen(() => this._performQuery<T>(text, args, resolvers))

        return this._performQuery<T>(text, args, resolvers)
    }


    private _performQuery<T extends Row>(text: QueryText, args: unknown[], resolvers: Resolvers<Future<T[], PostgresError>>): Future<T[], PostgresError> {
        this._logQuery(text, args)

        const parsed = this._parsed.get(text)
        if (parsed) {
            const query = new CollectQuery<T>(
                text, args, parsed, resolvers, this.config.queryTimeout
            )

            const err = this._registerQuery(query)

            if (err) {
                query.error(err)
                this._logError(err)
            }

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

            this._registerQuery(parseQuery)
            
            this._parsing.set(text, parseQuery.resolvers.future)
        }

        const parsing = this._parsing.get(text)!

        return parsing.andThen(meta => {
            const query = new CollectQuery<T>(
                text, args, meta, resolvers, this.config.queryTimeout
            )
            const err = this._registerQuery(query)

            if (err) {
                query.error(err)
                this._logError(err)
            }

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
        if (this.isClosed) return Future.reject(ErrConnectionClosed)

        const {text, args} = compileSqlTemplate(templates, params)

        const resolvers = Future.withResolvers<void, PostgresError>()
            
        if (this._reconnecting) return this._reconnecting.andThen(() => this._performExecute(text, args, resolvers))

        return this._performExecute(text, args, resolvers)
    }


    private _performExecute(text: QueryText, args: unknown[], resolvers: Resolvers<Future<void, PostgresError>>): Future<void, PostgresError> {
        this._logQuery(text, args)

        const parsed = this._parsed.get(text)

        if (parsed) {
            const query = new ExecuteQuery(
                text, args, parsed, resolvers, this.config.queryTimeout
            )

            const err = this._registerQuery(query)

            if (err) {
                query.error(err)
                this._logError(err)
            }

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

            this._registerQuery(parseQuery)
            
            this._parsing.set(text, parseQuery.resolvers.future)
        }

        const parsing = this._parsing.get(text)!
        
        return parsing.andThen(meta => {
            const query = new ExecuteQuery(
                text, args, meta, resolvers, this.config.queryTimeout
            )

            const err = this._registerQuery(query)

            if (err) {
                query.error(err)
                this._logError(err)
            }

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
        if (this.isClosed) throw ErrConnectionClosed

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
                .tapErr(err => controller.error(err))

            return stream
        }

        this._performStream<T>(text, args, controller)

        return stream
    }
    

    private _performStream<T extends Row>(text: QueryText, args: unknown[], controller: ReadableStreamDefaultController<T>) {        
        this._logQuery(text, args)

        const parsed = this._parsed.get(text)
        if (parsed) {
            const query = new StreamQuery<T>(
                text, args, parsed, controller, 
                this.config.queryTimeout
            )

            const err = this._registerQuery(query)

            if (err) {
                controller.error(err)
                this._logError(err)
            }

            return
        }

        if (!this._parsing.has(text)) {
            const newMeta = {statement: this._nextStatement(), columns: EMPTY_ARRAY, parameters: EMPTY_ARRAY}

            const parseQuery = new ParseQuery(
                text, newMeta, Future.withResolvers(), this.config.queryTimeout 
            )

            this._registerQuery(parseQuery)
            
            this._parsing.set(text, parseQuery.resolvers.future)
        }

        const parsing = this._parsing.get(text)!

        parsing
            .tap(meta => {
                const query = new StreamQuery(
                    text, args, meta, controller, 
                    this.config.queryTimeout
                )

                const err = this._registerQuery(query)

                if (err) {
                    controller.error(err)
                    this._logError(err)
                }
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
    begin<T>(txCallback: (transaction: Transaction) => Promise<T>) {
        if (this.isClosed) return Future.reject(ErrConnectionClosed)


        return Begin()
            .andThen(() => this.execute`begin`)    
            .andThen(() =>  {
                const tx = new Transaction(this)

                return Future.of(() => txCallback(tx))
                    .tap(() => {
                        if (tx.isActive) return tx.commit()
                    })
                    .tapErr(() => {
                        if (tx.isActive) return tx.rollback()
                    })
            })
    }


    /** Sends a `pg_notify` message on `channelName` (payload ≤ 8000 bytes). */
    notify(channelName: string, payload: string = "") {
        if (this.isClosed) return Future.reject(ErrConnectionClosed)

        return this.execute`select pg_notify(${channelName}, ${payload})`.map(() => {})
    }


    /** Subscribes `callback` to `channelName`, issuing `LISTEN` on first subscription. */
    listen(channelName: string, callback: (payload: string) => void) {
        if (this.isClosed) return Future.reject(ErrConnectionClosed)

        if (!this._listeningCallbacks.has(channelName as ChannelName)) {
            this._listeningCallbacks.set(channelName as ChannelName, new Set())
        }

        const callbackSet = this._listeningCallbacks.get(channelName as ChannelName)!

        callbackSet.add(callback)

        return this.execute`listen ${sql.ident(channelName)};`
    }


    /** Unsubscribes `callback`, issuing `UNLISTEN` once no callbacks remain. */
    unlisten(channelName: string, callback: (payload: string) => void): Future<void, PostgresError> {
        if (this.isClosed) return Future.reject(ErrConnectionClosed)

        if (!this._listeningCallbacks.has(channelName as ChannelName)) {
            return Ok()
        }

        const callbackSet = this._listeningCallbacks.get(channelName as ChannelName)!

        callbackSet.delete(callback)

        if (callbackSet.size === 0) {
            this._listeningCallbacks.delete(channelName as ChannelName)
            return this.execute`unlisten ${sql.ident(channelName)};`.map(() => {})
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
        this._socket.destroy()
        this._parsed.clear()
        this._parsing.clear()
        this._sheduled = false

        while (this._queue.hasMore) {
            this._queue.shift.error(ErrConnectionReconnecting)
        }
        this._writer.clear()

        
        return createSocket(this.config)
            .andThen(socket => upgradeSocket(socket, this.config))
            .andThen(socket => authorizeSocket(socket, this.config))
            .andThen(socket => {
                const connector = new SocketConnector(
                    socket, 
                    this._handlePacket.bind(this),
                    () => this._reconnect()
                )

                this._socket = connector

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

                this._logError(error)

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
                
                this._logNotice(notice)
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
            this._socket.destroy()
        })

        this._tryFinishClosing()

        return closing.future
    }
}
