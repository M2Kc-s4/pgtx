import type { Socket } from "bun"
import { DescribeType, ResponseType } from './constants'
import { ConnectionResponseBuffer } from './connection-response-reader'
import { ConnectionRequestBuffer } from './connection-request-writer'
import { PostgresError } from '../error'
import { CollectQuery, ExecuteQuery, ParseQuery, StreamQuery } from '../query'

export type SocketData = {
    onData?: (buffer: Buffer) => void
    onClose?: () => void
    onError?: (error: Error) => void
    onDrain?: () => void
    onHandshake?: (socket: PgSocket, success: boolean, error: Error | null) => void
}

export type PgSocket = Socket<SocketData>


export class SocketConnector {
    private _requestBuffer = ConnectionRequestBuffer.new(65536)
    private _residualResponseBuffer: Buffer | null = null
    private _backlog: Buffer | null = null
    private _scheduled = false
    private _closed = false

    private _onClose: () => void
    private _onData: (buffer: Buffer) => void
    private _onDrain: () => void


    constructor(
        private _socket: PgSocket,
        onData: (type: ResponseType, length: number, reader: ConnectionResponseBuffer) => void,
        onClose: () => void
    ) {
        this._onClose = () => {
            this._closed = true

            onClose()
        }

        this._onData = buffer => {
            const currentBuffer = this._residualResponseBuffer
                ? Buffer.concat([this._residualResponseBuffer, buffer])
                : buffer

            this._residualResponseBuffer = null

            const reader = ConnectionResponseBuffer.from(currentBuffer)

            while (reader.hasMore()) {
                if (!reader.hasFullPacket()) {
                    this._residualResponseBuffer = reader.getResidualBuffer()
                    return
                }

                const { type, length } = reader.readType()
                onData(type, length, reader)
            }
        }
        this._onDrain = () => {
            if (!this._backlog) return

            const backlog = this._backlog
            this._backlog = null
            this._write(backlog)
        }

        this._socket.setKeepAlive?.(true, 10000)
        this._socket.data = {
            onData: this._onData,
            onClose: this._onClose,
            onDrain: this._onDrain,
            onError: () => {},
        }
    }


    private _write(buffer: Uint8Array) {
        if (this._backlog) {
            this._backlog = Buffer.concat([this._backlog, buffer])
            return
        }

        const written = this._socket.write(buffer)

        if (written < buffer.length) {
            this._backlog = Buffer.from(buffer.subarray(Math.max(written, 0)))
        }
    }


    private _shedule() {
        if (!this._scheduled) {
            this._requestBuffer.clear()
            this._scheduled = true

            setImmediate(() => {
                if (this.isClosed) return

                this._scheduled = false
                this._requestBuffer.hasMore && this._write(this._requestBuffer.asBuffer())
                this._requestBuffer.clear()
            })
        }
    }

    writeNowait(request: ConnectionRequestBuffer) {
        this._write(request.asBuffer())
    }


    writeParse(query: ParseQuery) {
        this._shedule()

        this._requestBuffer
            .writeParse(query.meta.statement, query.text)
            .writeDescribe(DescribeType.Statement, query.meta.statement)
            .writeSync()
    }


    writeQuery(query: CollectQuery<any> | StreamQuery<any> | ExecuteQuery): PostgresError | null {
        this._shedule()

        const err = this._requestBuffer
            .writeBind("", query.meta, query.args)

        if (err) return err

        this._requestBuffer
            .writeExecute("")
            .writeSync()

        return null
    }


    unwrapSocket() {
        this._socket.data = {}

        return this._socket
    }


    close() {
        this._socket.terminate()
    }

    get isClosed() {
        return this._closed
    }
}
