import { Socket } from 'net'
import { DescribeType, ResponseType } from './constants'
import { ConnectionResponseBuffer } from './connection-response-reader'
import { ConnectionRequestBuffer } from './connection-request-writer'
import { PostgresError } from '../error'
import { CollectQuery, ExecuteQuery, ParseQuery, StreamQuery } from '../query'


export class SocketConnector {
    private _requestBuffer = ConnectionRequestBuffer.new(65536)
    private _residualResponseBuffer: Buffer | null = null
    private _scheduled = false
    private _closed = false

    private _onClose: () => void
    private _onData: (buffer: Buffer) => void


    constructor(
        private _socket: Socket,
        onData: (type: ResponseType, length: number, reader: ConnectionResponseBuffer) => void,
        onClose: () => void
    ) {
        this._onClose = () => {
            this._closed = true

            onClose()
        }

        this._onData = buffer => {
            const currentBuffer = this._residualResponseBuffer 
                ? Buffer.concat([this._residualResponseBuffer, buffer as Buffer]) 
                : buffer as Buffer
                
            this._residualResponseBuffer = null

            const reader = ConnectionResponseBuffer.from(currentBuffer) 

            while (reader.hasMore()) {
                if (!reader.hasFullPacket()) {
                    this._residualResponseBuffer = reader.getResidualBuffer()
                    return
                }

                const {type, length} = reader.readType()
                onData(type, length, reader)
            }
        }        

        this._socket.setKeepAlive(true, 10000)
        this._socket.on('data', this._onData)
        this._socket.on('error', () => {})
        this._socket.on('close', this._onClose)
    }


    private _shedule() {
        if (!this._scheduled) {
            this._requestBuffer.clear()
            this._scheduled = true
            
            setImmediate(() => {
                if (this.isClosed) return

                this._scheduled = false
                this._requestBuffer.hasMore && this._socket.write(this._requestBuffer.asBuffer())
                this._requestBuffer.clear()
            })
        }
    }

    writeNowait(request: ConnectionRequestBuffer) {
        this._socket.write(request.asBuffer())
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
        this._socket.off("data", this._onData)
        this._socket.off('close', this._onClose)

        return this._socket
    }


    close() {
        this._socket.destroy()
    }

    get isClosed() {
        return this._closed
    }
}
