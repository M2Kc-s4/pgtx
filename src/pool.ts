import { Connection } from "./connection"
import { Queue, RingQueue } from "./queue";
import { Begin, Future, Ok } from "fluent-future";
import { ErrPoolClosed, PostgresError } from "./error";
import { PoolConfig, PoolPartialConfig, Row, Waiter } from "./types";


/**
 * Connection pool and the main entry point for Pgtx.
 *
 * @example
 * const pool = new Pool({ host: 'localhost', user: 'postgres', password: 'postgres', database: 'test', max: 10 })
 * const users = await pool.query`SELECT * FROM users WHERE id = ${1}`
 * await pool.begin(async tx => tx.query`INSERT INTO users ...`)
 * await pool.close()
 */
export class Pool {
    private _available: RingQueue<Connection>
    private config: PoolConfig
    private _total = 0
    private _waiting = new Queue<Waiter>()
    private _isOpened = true

    constructor(config: PoolPartialConfig) {
        this.config =  {
            ...config, 
            max: config.max || 20
        }
        this._available = new RingQueue(this.config.max)
    }


    /**
     * Acquires a dedicated connection. Call `pool.release(conn)` when done —
     * prefer `pool.query()`/`pool.begin()` where possible, they release automatically.
     */
    acquire() {       
        if (!this._isOpened) return Future.reject(ErrPoolClosed)

        while (this._available.hasMore) {
            const conn = this._available.shift

            if (conn.isOpened) return Ok(conn)
            
            this._total--
        }

        if (this._total < this.config.max) {
            this._total++

            return Connection.connect(this.config)
                .tap(conn => {
                    if (!this.config.defaultHandlers) return 

                    const handlers = this.config.defaultHandlers
                    if (handlers.close) conn.on('close', handlers.close)
                    if (handlers.error) conn.on('error', handlers.error)
                    if (handlers.notice) conn.on('notice', handlers.notice)
                    if (handlers.notify) conn.on('notify', handlers.notify)
                    if (handlers.query) conn.on('query', handlers.query)
                })
                .tapErr(() => this._total--)
        }

        const {future, reject, resolve} = Future.withResolvers<Connection, PostgresError>()

        this._waiting.push({ resolve, reject })

        return future
    }


    /**
     * Runs `fn` with a borrowed connection and releases it afterward, even on error.
     *
     * @example
     * const status = await pool.withAcquire(conn => conn.query`SELECT pg_is_in_recovery()`)
     */
    withAcquire<T>(fn: (conn: Connection) => Promise<T>) {
        return Begin()
            .andThen(() => this.acquire())
            .andThen(conn => 
                Future.of(fn(conn))
                    .finally(() => this.release(conn))
            )
    }


    /** Returns `conn` to the pool, or hands it directly to the next waiting `acquire()`. */
    release(conn: Connection) {
        if (!this._isOpened) {
            void conn.close()
            return
        }

        if (conn.isClosed) {
            this._total--

            if (this._waiting.hasMore) {
                const waiter = this._waiting.shift

                this.acquire()
                    .tap(c => waiter.resolve(c))
                    .tapErr(err => waiter.reject(err))
                    .recover()
            }

            return
        }

        if (this._waiting.hasMore) {
            this._waiting.shift.resolve(conn)
            return
        }

        this._available.push(conn)
    }


    /**
     * Runs `txCallback` inside a transaction on a borrowed connection, releasing it afterward.
     *
     * @example
     * await pool.begin(async tx => {
     *   await tx.query`UPDATE accounts SET balance = balance - 10 WHERE id = 1`
     * })
     */
    begin<T>(txCallback: (transaction: Connection) => Promise<T>) {
        return Begin()
            .andThen(() => this.acquire())
            .andThen(conn => 
                conn.begin(txCallback)
                    .finally(() => this.release(conn))
            )
    }

    
    /**
     * Runs a one-off query on a borrowed connection.
     *
     * @example
     * const users = await pool.query<User>`SELECT * FROM users WHERE id = ${1}`
     */
    query<T extends Row>(templates: TemplateStringsArray, ...args: any[]) {
        if (!this._isOpened) return Future.reject(ErrPoolClosed)

        while (this._available.hasMore) {
            const conn = this._available.shift

            if (conn.isClosed) {
                this._total--
                continue
            }

            this._available.push(conn)
            return conn.query<T>(templates, ...args)
        }

        return this.acquire()
            .andThen(conn => {
                return conn.query<T>(templates, ...args)
                    .finally(() => this.release(conn))
            })
    }


    /** Like {@link query}, but for statements that don't return rows. */
    execute(templates: TemplateStringsArray, ...params: any[]) {
        if (!this._isOpened) return Future.reject(ErrPoolClosed)

        while (this._available.hasMore) {
            const conn = this._available.shift

            if (conn.isClosed) {
                this._total--
                continue
            }

            this._available.push(conn)
            return conn.execute(templates, ...params)
        }

        return this.acquire()
            .andThen(conn => {
                return conn.execute(templates, ...params)
                    .finally(() => this.release(conn))
            })
    }


    /**
     * Streams query results as a `ReadableStream`, without buffering rows in memory.
     *
     * @example
     * for await (const row of pool.stream<User>`SELECT * FROM orders`) { ... }
     */
    stream<T extends Row>(templates: TemplateStringsArray, ...params: any[]): ReadableStream<T> {  
        if (!this._isOpened) throw ErrPoolClosed

        while (this._available.hasMore) {
            const conn = this._available.shift

            if (conn.isClosed) {
                this._total--
                continue
            }

            this._available.push(conn)
            return conn.stream<T>(templates, ...params)
        }

        let controller!: ReadableStreamDefaultController<T>
        
        const stream = new ReadableStream<T>({
            start: c =>  {
                controller = c
            }
        })

        this.acquire()
            .tap(conn => {
                conn['_streamWithController']<T>(templates, params, controller)
                this.release(conn) 
            })
            .catch(err => {
                controller.error(err)
            })

        return stream
    }



    /** Number of idle connections. */
    get size() {
        return this._available.size
    }


    /** Total connections managed (idle + in use). */
    get total() {
        return this._total
    }


    get isOpened() {
        return this._isOpened
    }


    get isClosed() {
        return !this._isOpened
    }

    
    /**
     * Closes idle connections and rejects pending `acquire()` calls. Not usable afterward.
     */
    close() {
        this._isOpened = false

        const futures: Future<void, PostgresError>[] = []

        while (this._available.hasMore) {
            futures.push(this._available.shift.close())
        }

        while (this._waiting.hasMore) {
            this._waiting.shift.reject(ErrPoolClosed)
        }

        this._total = 0

        return Future.all(futures).map(() => {})
    }
}