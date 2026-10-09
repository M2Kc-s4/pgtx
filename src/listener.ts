import { Err, Future, Ok } from "fluent-future"
import { Connection } from "./connection"
import { sql } from "."
import { safe } from "./utils"
import { ErrConnectionClosed, PostgresError } from "./error"
import { ConnectionPartialConfig } from "./types"

type Handler = (payload: string) => void

export class Listener {
    private _connection: Connection | null = null
    private _connecting: Future<Connection, PostgresError> | null = null
    private _timer: NodeJS.Timeout | null = null
    private _closed = false
    private _retry = 0
    private _handlers = new Map<string, Set<Handler>>()

    constructor(private config: ConnectionPartialConfig) {}


    /** Subscribes `handler` to `channel`, connecting first if needed. */
    listen(channel: string, handler: Handler): Future<void, PostgresError> {
        if (this._closed) return Err(ErrConnectionClosed)

        let handlers = this._handlers.get(channel)
        if (!handlers) this._handlers.set(channel, handlers = new Set())
        handlers.add(handler)

        return this._connect()
            .andThen(conn => conn.execute`LISTEN ${sql.ident(channel)}`)
            .tapErr(() => {
                handlers.delete(handler)
                if (!handlers.size && this._handlers.get(channel) === handlers) this._handlers.delete(channel)
            })
    }


    /** Removes `handler` (or all handlers if omitted); sends UNLISTEN when the channel has none left. */
    unlisten(channel: string, handler?: Handler): Future<void, PostgresError> {
        const handlers = this._handlers.get(channel)
        if (!handlers) return Ok()

        if (handler) handlers.delete(handler)
        else handlers.clear()

        if (handlers.size) return Ok()

        this._handlers.delete(channel)

        return this._connection?.isOpened
            ? this._connection.execute`UNLISTEN ${sql.ident(channel)}`
            : Ok()
    }


    private _connect(): Future<Connection, PostgresError> {
        if (this._connection?.isOpened) return Ok(this._connection)

        if (this._connecting) return this._connecting

        const retry = () => {
            this._connection = null

            if (this._closed || this._timer || !this._handlers.size) return

            const delay = Math.min(1000 * 2 ** this._retry++, 30_000)

            this._timer = setTimeout(() => {
                this._timer = null
                this._connect().recover()
            }, delay)
        }

        this._connecting = Connection.connect(this.config)
            .andThen(conn => {
                if (this._closed) {
                    void conn.close()
                    return Err(ErrConnectionClosed)
                }

                conn.on('notify', (channel, payload) => {
                    this._handlers.get(channel)?.forEach(h => safe(h, payload))
                })
                conn.on('close', () => { if (this._connection === conn) retry() })

                return Future.all([...this._handlers.keys()].map(ch => conn.execute`LISTEN ${sql.ident(ch)}`))
                    .tapErr(() => void conn.close())
                    .map(() => conn)
            })
            .tap(conn => {
                this._connection = conn
                this._retry = 0
            })
            .tapErr(retry)
            .finally(() => { this._connecting = null })

        return this._connecting
    }


    get isClosed() {
        return this._closed
    }


    get isOpened() {
        return !this._closed
    }


    /** Stops reconnecting and closes the connection. Not usable afterward. */
    close() {
        this._closed = true
        this._handlers.clear()

        if (this._timer) clearTimeout(this._timer)

        return this._connection?.close() ?? Future.resolve()
    }
}