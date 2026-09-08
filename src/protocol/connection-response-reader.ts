import { AuthenticationCode, DataTypeOid, DataTypeOids, INT4Length, ResponseType, TransactionStatus } from "./constants"
import { ChannelName, ColumnDescription, ParameterDescription } from "../types"
import { PostgresError } from "../error"

const POSTGRES_EPOCH_MS = 946684800000


export class ConnectionResponseBuffer {
    private caret = 0

    private constructor(
        private buffer: Buffer
    ) {}

    
    static from(buffer: Buffer) {
        return new ConnectionResponseBuffer(buffer)
    }


    readChar(): string {
        const char = String.fromCharCode(this.buffer[this.caret])
        this.caret += 1
        return char
    }


    readInt32(): number {
        const value = this.buffer.readInt32BE(this.caret)
        this.caret += 4
        return value
    }

    
    readInt16(): number {
        const value = this.buffer.readInt16BE(this.caret)
        this.caret += 2
        return value
    }


    readCString(): string {
        const end = this.buffer.indexOf(0, this.caret)
        const text = this.buffer.toString('utf-8', this.caret, end)
        this.caret = end + 1
        return text
    }

    
    readRawString(length: number): string {
        const text = this.buffer.toString('utf-8', this.caret, this.caret + length)
        this.caret += length
        return text
    }


    hasMore(): boolean {
        return this.caret < this.buffer.length
    }


    readBinaryDate(): Date {
        const daysSince2000 = this.readInt32()
        const ms = POSTGRES_EPOCH_MS + (daysSince2000 * 86400000)
        return new Date(ms)
    }


    readByte() {
        return this.buffer[this.caret++]
    }


    readBinaryTimestamp(): Date {
        const microSecondsSince2000 = this.readInt64()
        const msSince2000 = Number(microSecondsSince2000 / 1000)
        return new Date(POSTGRES_EPOCH_MS + msSince2000)
    }


    readBinaryTime(): string {
        const microSecondsSinceMidnight = this.readInt64()
        const totalMs = Number(microSecondsSinceMidnight / 1000)
        
        const seconds = Math.floor(totalMs / 1000)
        const ms = totalMs % 1000
        const minutes = Math.floor(seconds / 60)
        const hours = Math.floor(minutes / 60)

        return `${String(hours).padStart(2, '0')}:${String(minutes % 60).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}.${String(ms).padStart(3, '0')}`
    }


    skipBytes(byteCount: number) {
        this.caret += byteCount
    }


    hasFullPacket(): boolean {
        const availableBytes = this.buffer.length - this.caret

        if (availableBytes < 5) return false

        const packetLength = this.buffer.readInt32BE(this.caret + 1)

        if (availableBytes < 1 + packetLength) return false

        return true
    }

    
    getResidualBuffer() {
        return this.buffer.subarray(this.caret)
    }

    readBool() {
        const value = this.buffer[this.caret] !== 0
        this.caret++
        return value
    }

    readBigInt64(): bigint {
        const value = this.buffer.readBigInt64BE(this.caret)
        this.caret += 8
        return value
    }


    readInt64() {
        const hi = this.buffer.readInt32BE(this.caret)
        const lo = this.buffer.readUInt32BE(this.caret + 4)
        this.caret += 8

        return (hi * 4294967296) + lo
    }


    readNumeric(): string {
        const ndigits = this.readInt16()
        const weight = this.readInt16()
        const sign = this.readInt16()
        const dscale = this.readInt16()

        if (sign === 0xC000 || sign === -16384) return 'NaN'

        const digits = new Array<number>(ndigits)
        for (let i = 0; i < ndigits; i++) {
            digits[i] = this.readInt16()
        }

        const highExp = Math.max(weight, 0)
        const lowExp = Math.min(weight - ndigits + 1, -1)

        let intPart = ''
        for (let exp = highExp; exp >= 0; exp--) {
            const idx = weight - exp
            const digit = (idx >= 0 && idx < ndigits) ? digits[idx] : 0
            intPart += exp === highExp ? String(digit) : String(digit).padStart(4, '0')
        }
        if (intPart === '') intPart = '0'

        let fracPart = ''
        for (let exp = -1; exp >= lowExp; exp--) {
            const idx = weight - exp
            const digit = (idx >= 0 && idx < ndigits) ? digits[idx] : 0
            fracPart += String(digit).padStart(4, '0')
        }

        if (dscale > 0) {
            fracPart = fracPart.padEnd(dscale, '0').slice(0, dscale)
        } else {
            fracPart = ''
        }

        const negative = sign === 0x4000 || sign === 16384
        const result = fracPart ? `${intPart}.${fracPart}` : intPart

        return negative ? `-${result}` : result
    }

    readBinaryTimetz(): string {
        const time = this.readBinaryTime()
        const zoneOffsetSeconds = this.readInt32()
        const offsetMinutesTotal = -zoneOffsetSeconds / 60
        const sign = offsetMinutesTotal >= 0 ? '+' : '-'
        const abs = Math.abs(offsetMinutesTotal)
        const offH = Math.floor(abs / 60)
        const offM = abs % 60
        return `${time}${sign}${String(offH).padStart(2, '0')}:${String(offM).padStart(2, '0')}`
    }


    readFloat32() {
        const value = this.buffer.readFloatBE(this.caret)
        this.caret += 4
        return value
    }


    readFloat64(): number {
        const value = this.buffer.readDoubleBE(this.caret)
        this.caret += 8
        return value
    }

    readUuid() {
        const buf = this.readBytes(16)

        return buf.toString('hex', 0, 4) + '-' +
                buf.toString('hex', 4, 6) + '-' +
                buf.toString('hex', 6, 8) + '-' +
                buf.toString('hex', 8, 10) + '-' +
                buf.toString('hex', 10, 16)
    }


    readBytes(length: number) {
        const result = Buffer.allocUnsafe(length)
        this.buffer.copy(result, 0, this.caret, this.caret + length)
        this.caret += length
        return result
    }


    readBinaryArray(fieldLength: number, elementParser: (length: number) => any): any[] {
        if (fieldLength === 0) return []

        const ndims = this.readInt32()
        const hasNull = this.readInt32()
        const elementOid = this.readInt32()

        if (ndims === 0) return []
        if (ndims > 1) {
            for (let d = 1; d < ndims; d++) {
                this.readInt32()
                this.readInt32()
            }
        }

        const dimLength = this.readInt32()
        const lbound = this.readInt32()

        const result = new Array(dimLength)
        for (let i = 0; i < dimLength; i++) {
            const itemLength = this.readInt32()
            if (itemLength === -1) {
                result[i] = null
            } else {
                result[i] = elementParser(itemLength)
            }
        }
        return result
    }


    readBinaryInet(fieldLength: number): string {
        const family = this.readByte()
        const bits = this.readByte()
        const isCidr = this.readByte()
        const nb = this.readByte()

        if (nb === 4) {
            const b1 = this.readByte()
            const b2 = this.readByte()
            const b3 = this.readByte()
            const b4 = this.readByte()
            return `${b1}.${b2}.${b3}.${b4}`
        } else if (nb === 16) {
            const parts: string[] = [];
            for (let i = 0; i < 8; i++) {
                parts.push(this.readInt16().toString(16))
            }
            return parts.join(':')
        }
        
        this.skipBytes(fieldLength - 4)
        return ''
    }

    
    readBinaryPoint(): { x: number; y: number } {
        return {
            x: this.readFloat64(),
            y: this.readFloat64()
        };
    }



    readType() {
        return {type: this.readByte() as ResponseType, length: this.readInt32() - INT4Length}
    }


    readAuthentication() {
        return this.readInt32() as AuthenticationCode
    }

    
    readMD5Salt() {
        return this.readBytes(4)
    }


    readParameterStatus() {
        return {
            name: this.readCString(), 
            value: this.readCString()
        }
    }


    readBackendKeyData() {
        return {
            PID: this.readInt32(), 
            secret: this.readInt32()
        }
    }


    readErrorResponse(): PostgresError {            
        let severity = ''
        let code = ''
        let message = ''
        let detail = ''
        let where = ''
        let hint = ''
        let position = ''
        let dataType = ''
        let constraint = ''

        while (true) {
            const marker = this.readChar()
            if (marker === '\0') break

            const text = this.readCString()

            switch (marker) {
                case 'S': severity = text; break;
                case 'C': code = text; break;
                case 'M': message = text; break;
                case 'D': detail = text; break;
                case 'W': where = text; break;
                case 'H': hint = text; break;        
                case 'P': position = text; break;    
                case 'd': dataType = text; break;    
                case 'n': constraint = text; break;  
            }
        }

        return new PostgresError(
            message, 
            code, 
            detail, 
            severity, 
            where, 
            hint, 
            position, 
            dataType, 
            constraint
        )
    }


    readReadyForQuery() {
        return this.readByte() as TransactionStatus
    }


    readSaslMechanisms(): string[] {
        const mechanisms: string[] = []

        while (true) {
            const mech = this.readCString()
            if (mech === "") break
            mechanisms.push(mech)
        }

        return mechanisms
    }


    readSaslMessage(length: number): string {
        return this.readRawString(length - 4)
    }


    readRowDescription() {
        const columnsCount = this.readInt16()
        const columns = new Array<ColumnDescription>(columnsCount)

        for (let i = 0; i < columnsCount; i++) {
            const name = this.readCString()
            this.readInt32()
            this.readInt16()

            columns[i] = {
                name: name,
                typeOID: this.readInt32() as DataTypeOid,
            }
            
            this.readInt32()
            this.readInt32()
        }

        return columns
    }


    readDataRow(descriptions: ColumnDescription[], int8toBigint: boolean): Record<string, any> {
        const fieldsCount = this.readInt16()
        const row: Record<string, any> = {}

        for (let i = 0; i < fieldsCount; i++) {
            const fieldLength = this.readInt32()
            const desc = descriptions[i]
            const key = desc.name

            if (fieldLength === -1) {
                row[key] = null
                continue
            }

            const oid = desc.typeOID

            switch (oid) {
                case DataTypeOids.Text:
                case DataTypeOids.Varchar:
                case DataTypeOids.Char:
                case DataTypeOids.Bpchar: {
                    row[key] = this.readRawString(fieldLength)
                } break

                case DataTypeOids.Int2: {
                    row[key] = this.readInt16()
                } break

                case DataTypeOids.Int4: {
                    row[key] = this.readInt32()
                } break

                case DataTypeOids.Int8: {
                    row[key] = int8toBigint ? this.readBigInt64() : this.readInt64()
                } break

                case DataTypeOids.Float4: {
                    row[key] = this.readFloat32()
                } break
                
                case DataTypeOids.Float8: {
                    row[key] = this.readFloat64()
                } break
                
                case DataTypeOids.Bool: {
                    row[key] = this.readBool()
                } break

                case DataTypeOids.Bytea: {
                    row[key] = this.readBytes(fieldLength)
                } break

                case DataTypeOids.Jsonb: {
                    this.skipBytes(1)
                    row[key] = JSON.parse(this.readRawString(fieldLength - 1))
                } break

                case DataTypeOids.Json: {
                    row[key] = JSON.parse(this.readRawString(fieldLength))
                } break

                case DataTypeOids.Date: {
                    row[key] = this.readBinaryDate()
                } break

                case DataTypeOids.Timestamp:   
                case DataTypeOids.Timestamptz: {
                    row[key] = this.readBinaryTimestamp()
                } break

                case DataTypeOids.Uuid: {
                    row[key] = this.readUuid()
                } break

                case DataTypeOids.Time: {
                    row[key] = this.readBinaryTime()
                } break

                case DataTypeOids.Timetz: {
                    row[key] = this.readBinaryTimetz()
                } break

                case DataTypeOids.Numeric: {
                    row[key] = this.readNumeric()
                } break

                case DataTypeOids.Point: {
                    row[key] = this.readBinaryPoint()
                } break

                case DataTypeOids.Inet:
                case DataTypeOids.Cidr: {
                    row[key] = this.readBinaryInet(fieldLength)
                } break

                case DataTypeOids.Int4Array: {
                    row[key] = this.readBinaryArray(fieldLength, () => this.readInt32())
                } break

                case DataTypeOids.TextArray:
                case DataTypeOids.VarcharArray: { 
                    row[key] = this.readBinaryArray(fieldLength, (len) => this.readRawString(len))
                } break

                case DataTypeOids.BoolArray: {
                    row[key] = this.readBinaryArray(fieldLength, () => this.readBool())
                } break

                default: {
                    this.skipBytes(fieldLength)
                    row[key] = undefined
                } break
            }
        }

        return row
    }


    readNotificationResponse() {
        this.readInt32()
        const name = this.readCString() as ChannelName
        const payload = this.readCString()
        
        return {name, payload} 
    }


    readCommandComplete(): string {
        return this.readCString()
    }


    readParameterDescription(): ParameterDescription {
        const count = this.readInt16()

        const description = Array<DataTypeOid>(count)

        for (let i = 0; i < count; i++) {
            description[i] = this.readInt32() as DataTypeOid
        }

        return description
    }
}