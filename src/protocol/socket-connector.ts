import { Socket } from 'net'
import { DescribeType, ResponseType } from './constants'
import { ConnectionResponseBuffer } from './connection-response-reader'
import { ConnectionRequestBuffer } from './connection-request-writer'
import { ErrSocketFailed, PostgresError } from '../error'
import { ConnectorConfig } from '../types'
import { CollectQuery, ExecuteQuery, ParseQuery, StreamQuery } from '../query'
import { nextTick } from 'process'


const shedule = {
    Immediate: setImmediate,
    afterMicrotask: setTimeout,
    beforeMicrotask: nextTick
}

export class SocketConnector {
    private _requestBuffer = ConnectionRequestBuffer.new(65536)
    private _residualResponseBuffer: Buffer | null = null
    private _scheduled = false
    private _destroyed = false

    private _onError: (error: unknown) => void
    private _onClose: () => void
    private _onData: (buffer: Buffer) => void


    constructor(
        private _config: ConnectorConfig,
        private _socket: Socket,
        onData: (type: ResponseType, length: number, reader: ConnectionResponseBuffer) => void,
        onError: (error: unknown) => void
    ) {        
        this._onError = (err) => {
            if (this._destroyed) return

            this._destroyed = true
            onError(err)
            this.destroy()
        }

        this._onClose = () => {
            if (this._destroyed) return
            this._destroyed = true
            onError(ErrSocketFailed)
            this.destroy()
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
        this._socket.on('error', this._onError)
        this._socket.on('close', this._onClose)
    }


    private _shedule() {
        if (!this._scheduled) {
            this._requestBuffer.clear()
            this._scheduled = true
            
            shedule[this._config.syncSсhedule](() => {
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
        this._socket.off('error', this._onError)
        this._socket.off("data", this._onData)
        this._socket.off('close', this._onClose)

        return this._socket
    }


    destroy() {
        this._socket.destroy()
    }
}
