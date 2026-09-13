import { Connection } from "./connection"
import { Transaction } from "./transaction"
import { Queue, RingQueue } from "./queue";
import { Begin, Future, Ok } from "fluent-future";
import { ErrPoolClosed, PostgresError } from "./error";
import { PoolConfig, PoolPartialConfig, Row, Waiter } from "./types";
import { compileSqlTemplate } from "./utils";


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
        const conf: PoolConfig =  {...config, max: config.max || 20}
        this.config = conf
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

            if (conn.isOpened) {
                return Ok(conn)
            }
            this._total--
        }

        if (this._total < this.config.max) {
            this._total++
            return Connection.new(this.config)
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
            throw ErrPoolClosed
        }

        if (!conn.isOpened) {
            this._total--
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
    begin<T>(txCallback: (transaction: Transaction) => Promise<T>) {
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

            if (!conn.isOpened) {
                this._total--
                continue
            }

            this._available.push(conn)
            return conn.query<T>(templates, ...args)
        }

        return this.acquire()
            .andThen(conn => {
                this.release(conn) 
                return conn.query<T>(templates, ...args)
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
                this.release(conn)
                return conn.execute(templates, ...params)
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

            if (!conn.isOpened) {
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
                this.release(conn) 
                const {text, args} = compileSqlTemplate(templates, params)
                
                conn['_performStream']<T>(text, args, controller)
            })
            .catch(err => {
                controller.error(err)
            })

        return stream
    }


    /** Sends a `pg_notify` message on `channelName` (payload ≤ 8000 bytes). */
    notify(channelName: string, payload: string = "") {
        return this.acquire()
            .andThen(conn => conn.notify(channelName, payload)    
                .finally(() => this.release(conn))
            )
    }


    /**
     * Subscribes `callback` to `channel` on a dedicated connection.
     * Returns an unsubscribe function that issues `UNLISTEN` and releases the connection.
     *
     * @example
     * const unlisten = await pool.listen('order_created', payload => console.log(payload))
     * await unlisten()
     */
    listen(channel: string, callback: (payload: string) => void) {
        return this.acquire()
            .andThen(conn => 
                conn.listen(channel, callback)
                    .map(() => () => 
                        conn.unlisten(channel, callback)
                            .tap(() => this.release(conn))
                    )
            )
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