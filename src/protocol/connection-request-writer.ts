import { ParameterDescription } from "../types"
import { prepareValue } from "../utils"
import { DataTypeOid, DataTypeOids, DescribeType, RequestType, RequestTypes } from "./constants"


const POSTGRES_EPOCH = Date.UTC(2000, 0, 1)


export class ConnectionRequestBuffer {
    private constructor(
        private buffer: Buffer,
        private offset = 0,
        private lastRequestLenByteOffset = 0
    ) {}


    static new(capacity: number) {
        return new ConnectionRequestBuffer(Buffer.allocUnsafe(capacity))
    }

    
    private ensureCapacity(needed: number) {
        const required = needed + this.offset

        if (required <= this.buffer.length) return
        

        let newCapacity = this.buffer.length * 2
        if (newCapacity < required) {
            newCapacity = required
        }

        const buffer = Buffer.alloc(newCapacity)

        this.buffer.copy(buffer, 0, 0, this.offset)
        this.buffer = buffer

        return this
    }


    writeCString(string: string) {
        this.ensureCapacity(Buffer.byteLength(string) + 1)

        this.offset += this.buffer.write(string, this.offset, 'utf-8')

        this.buffer[this.offset] = 0
        this.offset++

        return this
    }


    writeString(string: string) {
        this.ensureCapacity(Buffer.byteLength(string))

        this.offset += this.buffer.write(string, this.offset, 'utf-8')

        return this
    }


    writeInt16(number: number) {
        this.ensureCapacity(2)

        this.buffer.writeInt16BE(number, this.offset)
        this.offset += 2

        return this
    }


    writeInt32(number: number) {
        this.ensureCapacity(4)

        this.buffer.writeInt32BE(number, this.offset)
        this.offset += 4

        return this
    }


    writeChar(char: string) {
        this.ensureCapacity(1)
        this.buffer[this.offset] = char.charCodeAt(0)
        this.offset++

        return this
    }


    writeByte(byte: number) {
        this.ensureCapacity(1)
        this.buffer[this.offset++] = byte

        return this
    }


    writeBigInt64(value: bigint) {
        this.ensureCapacity(8)
        this.buffer.writeBigInt64BE(value, this.offset)
        this.offset += 8

        return this
    }


    writeUInt16(value: number) {
        this.ensureCapacity(2)
        this.buffer.writeUInt16BE(value, this.offset)
        this.offset += 2

        return this
    }


    writeUInt32(value: number) {
        this.ensureCapacity(4)
        this.buffer.writeUInt32BE(value, this.offset)
        this.offset += 4

        return this
    }


    writeBytes(value: Uint8Array) {
        this.ensureCapacity(value.byteLength)

        this.buffer.set(
            value,
            this.offset
        )

        this.offset += value.byteLength
        return this
    }


    writeInt64(number: number) {
        this.ensureCapacity(8)
        this.buffer.writeBigInt64BE(BigInt(number), this.offset)
        this.offset += 8
        return this
    }


    writeFloat32(number: number) {
        this.ensureCapacity(4)
        this.buffer.writeFloatBE(number, this.offset)
        this.offset += 4
        return this
    }


    writeFloat64(number: number) {
        this.ensureCapacity(8)
        this.buffer.writeDoubleBE(number, this.offset)
        this.offset += 8
        return this
    }


    startRequest(requestType: RequestType) {
        this.writeByte(requestType)

        this.lastRequestLenByteOffset = this.offset
        return this.writeInt32(0)
    }


    startMessage() {
        this.lastRequestLenByteOffset = this.offset
        return this.writeInt32(0)
    }


    endRequest() {
        this.ensureCapacity(4)

        this.buffer.writeInt32BE(
            this.offset - (this.lastRequestLenByteOffset), 
            this.lastRequestLenByteOffset
        )

        return this
    }


    asBuffer() {
        return this.buffer.subarray(0, this.offset)
    }


    clear() {
        this.offset = 0
        this.lastRequestLenByteOffset = 0
    }

    writeQuery(text: string) {
        this.startRequest(RequestTypes.SimpleQuery)
            .writeCString(text)
            .endRequest()
            
        return this
    }


    writeParse(name: string | "", text: string) {
        this.startRequest(RequestTypes.Parse)
            .writeCString(name)
            .writeCString(text)
            .writeInt16(0)
            .endRequest()
            
        return this
    }


    writeDescribe(
        type: DescribeType,
        name: string | ""
    ) {
        this.startRequest(RequestTypes.Describe)
            .writeChar(type)
            .writeCString(name)
            .endRequest()

        return this
    }


    writeClose(name: string) {
        this.startRequest(RequestTypes.Close)
            .writeChar("P")                        
            .writeCString(name)                     
            .endRequest()
        
            return this
    }


    writeStartup(user: string, database: string) {
        this.startMessage()
            .writeInt32(196608)
            .writeCString('user').writeCString(user)
            .writeCString('database').writeCString(database)
            .writeChar('\0')
            .endRequest()

        return this
    }


    writeExecute(portName: string | "") {
        this.startRequest(RequestTypes.Execute)
            .writeCString(portName)
            .writeInt32(0)
            .endRequest()

        return this
    }


    writeSync() {
        this.startRequest(RequestTypes.Sync).endRequest()
            
        return this
    }


    writeSSLRequest() {
        this.startMessage()
            .writeInt32(80877103)
            .endRequest()

        return this
    }


    writePassword(password: string) {
        this.startRequest(RequestTypes.Password)
            .writeCString(password)
            .endRequest()
        
        return this
    }

    
    writeSaslInitial(mechanism: string, clientFirstMessage: string) {
        this.startRequest(RequestTypes.Password)
            .writeCString(mechanism)
            .writeInt32(Buffer.byteLength(clientFirstMessage, 'utf-8'))
            .writeString(clientFirstMessage)
            .endRequest()

        return this
    }


    writeSaslResponse(clientFinalMessage: string) {
        this.startRequest(RequestTypes.Password)
            .writeString(clientFinalMessage)
            .endRequest()

        return this
    }


    writeBinaryBool(value: boolean) {
        this.writeInt32(1)
        this.writeByte(value ? 1 : 0)
        return this
    }


    writeBinaryInt2(value: number) {
        this.writeInt32(2)
        this.writeInt16(value)
        return this
    }


    writeBinaryInt4(value: number) {
        this.writeInt32(4)
        this.writeInt32(value)

        return this
    }


    writeBinaryInt8(value: number) {
        this.writeInt32(8)
        this.writeInt64(value)
        return this
    }


    writeBinaryBigInt8(value: bigint) {
        this.writeInt32(8)
        this.writeBigInt64(value)
        return this
    }


    writeBinaryFloat4(value: number) {
        this.writeInt32(4)
        this.writeFloat32(value)
        return this
    }


    writeBinaryFloat8(value: number) {
        this.writeInt32(8)
        this.writeFloat64(value)
        return this
    }


    writeBinaryBytea(value: Uint8Array) {
        this.writeInt32(value.byteLength)
        this.writeBytes(value)
        return this
    }


    writeBinaryTimestamp(value: Date) {
        const micros =
            BigInt(value.getTime() - POSTGRES_EPOCH) * 1000n

        this.writeInt32(8)
        this.writeBigInt64(micros)

        return this
    }


    writeNull() {
        this.writeInt32(-1)
        return this
    }


    private canWriteBinary(
        value: unknown,
        oid: number | undefined
    ): boolean {
        if (value == null || !oid) {
            return false
        }

        switch (oid) {
            case DataTypeOids.Bool:
                return typeof value === "boolean"


            case DataTypeOids.Int2:
                return (
                    typeof value === "number" &&
                    Number.isInteger(value) &&
                    value >= -32768 &&
                    value <= 32767
                )

            
            case DataTypeOids.Int4:
                return (
                    typeof value === "number" &&
                    Number.isInteger(value) &&
                    value >= -2147483648 &&
                    value <= 2147483647
                )


            case DataTypeOids.Int8:
                return (
                    typeof value === "bigint" ||
                    (
                        typeof value === "number" &&
                        Number.isSafeInteger(value)
                    )
                )

                
            case DataTypeOids.Float4:
            case DataTypeOids.Float8:
                return (
                    typeof value === "number" &&
                    Number.isFinite(value)
                )

            
            case DataTypeOids.Bytea:
                return (
                    Buffer.isBuffer(value) ||
                    value instanceof Uint8Array
                )


            case DataTypeOids.Timestamp:
            case DataTypeOids.Timestamptz:
                return value instanceof Date

            default:
                return false
        }
    }


        writeBind(
            portName: string | "",
            statementName: string | "",
            params: unknown[],
            parameterTypes?: ParameterDescription
        ) {
            const binary = params.map((value, i) =>
                this.canWriteBinary(value, parameterTypes?.[i])
            )

            const request = this.startRequest(RequestTypes.Bind)
                .writeCString(portName)
                .writeCString(statementName)
                .writeInt16(params.length)

            for (const isBinary of binary) {
                request.writeInt16(isBinary ? 1 : 0)
            }

            request.writeInt16(params.length)

            for (let i = 0; i < params.length; i++) {
                const value = params[i]

                if (value == null) {
                    request.writeNull()
                    continue
                }

                if (binary[i]) {                
                    switch (parameterTypes![i]) {
                        case DataTypeOids.Bool:
                            request.writeBinaryBool(value as boolean)
                            break

                        case DataTypeOids.Int2:
                            request.writeBinaryInt2(value as number)
                            break

                        case DataTypeOids.Int4:
                            request.writeBinaryInt4(value as number)
                            break

                        case DataTypeOids.Int8:
                            typeof value === "bigint"
                                ? request.writeBinaryBigInt8(value)
                                : request.writeBinaryInt8(value as number)
                            break

                        case DataTypeOids.Float4:
                            request.writeBinaryFloat4(value as number)
                            break

                        case DataTypeOids.Float8:
                            request.writeBinaryFloat8(value as number)
                            break

                        case DataTypeOids.Bytea:
                            request.writeBinaryBytea(value as Uint8Array)
                            break

                        case DataTypeOids.Timestamp:
                        case DataTypeOids.Timestamptz:
                            request.writeBinaryTimestamp(value as Date)
                            break
                    }

                    continue
                }

                const prepared = prepareValue(value)

                if (prepared === null) {
                    request.writeNull()
                } else {
                    request
                        .writeInt32(Buffer.byteLength(prepared))
                        .writeString(prepared)
                }
            }

            request
                .writeInt16(1)
                .writeInt16(1)
                .endRequest()

            return this
        }
    }