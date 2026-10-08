import { Err, Future, Ok } from "fluent-future"
import { Connection } from "./connection"
import { sql } from "."
import { ErrConnectionClosed, PostgresError } from "./error"
import { ConnectionPartialConfig } from "./types"

type Handler = (payload: string) => void

export type ListenerConfig = Omit<ConnectionPartialConfig, 'onNotify'>

/**
 * Dedicated LISTEN/NOTIFY subscriber. Owns one connection, reconnects with
 * backoff and re-issues LISTEN for every registered channel after a drop.
 * Notifications sent while the connection is down are lost (PostgreSQL semantics).
 *
 * @example
 * const listener = new Listener({ host: 'localhost', user: 'postgres', password: 'postgres', database: 'test', port: 5432 })
 * await listener.listen('orders', payload => console.log(payload))
 * await listener.close()
 */
export class Listener {
    private readonly _conn: Connection
    private readonly _handlers: Record<string, Handler> = {}
    private readonly _subscribed = new Set<string>()

    private _opening: Future<void, PostgresError> | null = null
    private _timer: NodeJS.Timeout | null = null
    private _closed = false
    private _retry = 0

    constructor(config: ListenerConfig) {
        this._conn = new Connection({
            ...config,
            onNotify: this._handlers,
            onClose: () => {
                this._subscribed.clear()
                config.onClose?.()
                this._reconnect()
            },
        })
    }


    /** Subscribes `cb` to `channel`. Connects first if needed. Replaces a previous handler for the same channel. */
    listen(channel: string, cb: Handler): Future<void, PostgresError> {
        if (this._closed) return Err(ErrConnectionClosed)

        this._handlers[channel] = cb

        return this._open()
            .tapErr(() => { if (this._handlers[channel] === cb) delete this._handlers[channel] })
    }


    /** Stops listening to `channel`. */
    unlisten(channel: string): Future<void, PostgresError> {
        delete this._handlers[channel]

        if (!this._subscribed.delete(channel) || !this._conn.isConnected) return Ok()

        return this._conn.execute`UNLISTEN ${sql.ident(channel)}`
    }


    /** Connects (if not connected) and makes sure every registered channel is LISTENed. */
    private _open(): Future<void, PostgresError> {
        if (this._closed) return Err(ErrConnectionClosed)

        if (this._opening) return this._opening.andThen(() => this._sync())

        if (this._conn.isConnected) return this._sync()

        this._clearTimer()

        this._opening = this._conn.connect()
            .tap(() => { this._retry = 0 })
            .tapErr(() => this._reconnect())
            .finally(() => { this._opening = null })

        return this._opening.andThen(() => this._sync())
    }


    /** Issues LISTEN for channels that aren't subscribed on the current session yet. */
    private _sync(): Future<void, PostgresError> {
        if (!this._conn.isConnected) return Ok()

        const pending = Object.keys(this._handlers).filter(ch => !this._subscribed.has(ch))

        for (const ch of pending) this._subscribed.add(ch)

        return Future.all(pending.map(ch =>
            this._conn.execute`LISTEN ${sql.ident(ch)}`
                .tapErr(() => this._subscribed.delete(ch))
        )).map(() => {})
    }


    private _reconnect() {
        if (this._closed || this._timer) return

        const delay = Math.min(1000 * 2 ** this._retry++, 30_000)

        this._timer = setTimeout(() => {
            this._timer = null
            this._open().recover()
        }, delay)
    }


    private _clearTimer() {
        if (!this._timer) return

        clearTimeout(this._timer)
        this._timer = null
    }


    get isConnected() {
        return this._conn.isConnected
    }


    close() {
        this._closed = true
        this._clearTimer()

        return this._conn.close()
    }
}