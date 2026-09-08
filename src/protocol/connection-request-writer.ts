import { createBindTypeError, PostgresError } from "../error"
import { StatementMeta } from "../types"
import { DataTypeOid, DataTypeOids, DescribeType, RequestType, RequestTypes } from "./constants"
import { handlers } from "./types"


const POSTGRES_EPOCH = Date.UTC(2000, 0, 1)


export class ConnectionRequestBuffer {
    private constructor(
        private buffer: Buffer,
        private offset = 0,
        private lastRequestLenByteOffset = 0,
        private _markedCaret: number = 0
    ) {}

    
    static new(capacity: number) {
        return new ConnectionRequestBuffer(Buffer.allocUnsafe(capacity))
    }


    get isEmpty() {
        return !this.buffer.length
    }

    get hasMore() {
        return !!this.buffer.length
    }


    mark() {
        this._markedCaret = this.offset
        return this
    }


    rollback() {
        this.offset = this._markedCaret
        return this
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

    // ------ Binary writers of js values into Bind parameters ------
    writeBinaryBool(value: boolean) {
        this.writeInt32(1)
        this.writeByte(value ? 1 : 0)
        return this
    }


    writeBinaryString(value: string) {
        this.writeInt32(Buffer.byteLength(value))
        this.writeString(value)
        
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


    writeBinaryPoint(value: { x: number; y: number }) {
        this.writeInt32(16)
        this.writeFloat64(value.x)
        this.writeFloat64(value.y)

        return this
    }


    writeBinaryNumeric(value: string) {
        const negative = value.startsWith('-')
        const unsigned = negative ? value.slice(1) : value

        const [intPartRaw, fracPartRaw = ''] = unsigned.split('.')
        const intPart = intPartRaw === '' ? '0' : intPartRaw
        const fracPart = fracPartRaw

        const dscale = fracPart.length

        const intPad = (4 - (intPart.length % 4)) % 4
        const paddedInt = '0'.repeat(intPad) + intPart

        const fracPad = (4 - (fracPart.length % 4)) % 4
        const paddedFrac = fracPart + '0'.repeat(fracPad)

        const digitsStr = paddedInt + paddedFrac
        const digits: number[] = []
        for (let i = 0; i < digitsStr.length; i += 4) {
            digits.push(parseInt(digitsStr.slice(i, i + 4), 10))
        }

        let weight = paddedInt.length / 4 - 1

        let start = 0
        while (start < digits.length && digits[start] === 0) {
            start++
            weight--
        }

        let end = digits.length
        while (end > start && digits[end - 1] === 0) {
            end--
        }

        const trimmed = digits.slice(start, end)
        const isZero = trimmed.length === 0

        this.writeInt32(8 + trimmed.length * 2)
        this.writeInt16(trimmed.length)
        this.writeInt16(isZero ? 0 : weight)
        this.writeUInt16(negative && !isZero ? 0x4000 : 0x0000)
        this.writeUInt16(dscale)

        for (const digit of trimmed) {
            this.writeInt16(digit)
        }

        return this
    }


    writeBinaryDate(value: Date) {
        const days = Math.round(
            (Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()) - POSTGRES_EPOCH)
            / 86_400_000
        )

        this.writeInt32(4)
        this.writeInt32(days)
        return this
    }


    writeBinaryTime(value: string) {
        const [time, ms] = value.split('.')
        const [hours, minutes, seconds] = time.split(':').map(Number)

        const micros =
            (hours * 3_600_000 +
                minutes * 60_000 +
                seconds * 1_000 +
                Number(ms)) *
            1000

        this.writeInt32(8)
        this.writeBigInt64(BigInt(micros))

        return this
    }

    writeBinaryTimetz(value: string) {
        const time = value.slice(0, 12)
        const offset = value.slice(12)

        const [hours, minutes, seconds] = time
            .slice(0, 8)
            .split(':')
            .map(Number)

        const ms = Number(time.slice(9, 12))

        const sign = offset[0]
        const offHours = Number(offset.slice(1, 3))
        const offMinutes = Number(offset.slice(4, 6))

        const micros =
            (hours * 3_600_000 +
                minutes * 60_000 +
                seconds * 1_000 +
                ms) *
            1000

        const offsetSeconds =
            (offHours * 60 + offMinutes) * 60 * (sign === '+' ? -1 : 1)

        this.writeInt32(12)
        this.writeBigInt64(BigInt(micros))
        this.writeInt32(offsetSeconds)

        return this
    }


    writeBinaryInterval(value: { months: number; days: number; microseconds: number | bigint }) {
        this.writeInt32(16)
        this.writeBigInt64(
            typeof value.microseconds === 'bigint'
                ? value.microseconds
                : BigInt(value.microseconds)
        )
        this.writeInt32(value.days)
        this.writeInt32(value.months)
        return this
    }
    

    writeBinaryJson(value: unknown) {
        const json = typeof value === 'string' ? value : JSON.stringify(value)
        this.writeInt32(Buffer.byteLength(json))
        this.writeString(json)
        return this
    }


    writeBinaryJsonb(value: unknown) {
        const json = typeof value === 'string' ? value : JSON.stringify(value)
        const byteLen = Buffer.byteLength(json)

        this.writeInt32(byteLen + 1)
        this.writeByte(1)
        this.writeString(json)
        return this
    }


    writeBinaryUuid(value: string) {
        const hex = value.replace(/-/g, '')
        this.writeInt32(16)
        this.writeBytes(Buffer.from(hex, 'hex'))
        return this
    }


    private static readonly PGSQL_AF_INET = 2
    private static readonly PGSQL_AF_INET6 = 3


    private ipv4ToBytes(addr: string): Uint8Array {
        return Uint8Array.from(addr.split('.').map(Number))
    }


    private ipv6ToBytes(addr: string): Uint8Array {
        const [head, tail] = addr.split('::')
        const headParts = head ? head.split(':') : []
        const tailParts = tail !== undefined ? (tail ? tail.split(':') : []) : []
        const missing = 8 - headParts.length - tailParts.length
        const groups = [...headParts, ...Array(Math.max(missing, 0)).fill('0'), ...tailParts]

        const bytes = new Uint8Array(16)
        groups.forEach((g, i) => {
            const val = parseInt(g || '0', 16)
            bytes[i * 2] = (val >> 8) & 0xff
            bytes[i * 2 + 1] = val & 0xff
        })
        return bytes
    }


    writeBinaryInet(value: string, isCidr: boolean = false) {
        const [addr, prefixStr] = value.split('/')
        const isIPv6 = addr.includes(':')
        const family = isIPv6 ? ConnectionRequestBuffer.PGSQL_AF_INET6 : ConnectionRequestBuffer.PGSQL_AF_INET
        const addrBytes = isIPv6 ? this.ipv6ToBytes(addr) : this.ipv4ToBytes(addr)
        const bits = prefixStr !== undefined ? parseInt(prefixStr, 10) : (isIPv6 ? 128 : 32)

        this.writeInt32(4 + addrBytes.length)
        this.writeByte(family)
        this.writeByte(bits)
        this.writeByte(isCidr ? 1 : 0)
        this.writeByte(addrBytes.length)
        this.writeBytes(addrBytes)
        return this
    }


    writeBinaryCidr(value: string) {
        return this.writeBinaryInet(value, true)
    }


    writeBinaryMacaddr(value: string) {
        const bytes = Uint8Array.from(value.split(':').map(h => parseInt(h, 16)))
        this.writeInt32(6)
        this.writeBytes(bytes)
        return this
    }


    writeBinaryOid(value: number) {
        this.writeInt32(4)
        this.writeUInt32(value)
        return this
    }


    writeBinaryXid(value: number) {
        return this.writeBinaryOid(value)
    }


    writeBinaryCid(value: number) {
        return this.writeBinaryOid(value)
    }


    writeBinaryRegproc(value: number) {
        return this.writeBinaryOid(value)
    }


    writeBinaryLseg(value: { a: { x: number; y: number }; b: { x: number; y: number } }) {
        this.writeInt32(32)
        this.writeFloat64(value.a.x)
        this.writeFloat64(value.a.y)
        this.writeFloat64(value.b.x)
        this.writeFloat64(value.b.y)
        return this
    }


    writeBinaryPath(value: { closed: boolean; points: { x: number; y: number }[] }) {
        this.writeInt32(1 + 4 + value.points.length * 16)
        this.writeByte(value.closed ? 1 : 0)
        this.writeInt32(value.points.length)
        for (const p of value.points) {
            this.writeFloat64(p.x)
            this.writeFloat64(p.y)
        }
        return this
    }


    writeBinaryBox(value: { high: { x: number; y: number }; low: { x: number; y: number } }) {
        this.writeInt32(32)
        this.writeFloat64(value.high.x)
        this.writeFloat64(value.high.y)
        this.writeFloat64(value.low.x)
        this.writeFloat64(value.low.y)
        return this
    }


    writeBinaryPolygon(value: { points: { x: number; y: number }[] }) {
        this.writeInt32(4 + value.points.length * 16)
        this.writeInt32(value.points.length)
        for (const p of value.points) {
            this.writeFloat64(p.x)
            this.writeFloat64(p.y)
        }
        return this
    }

    
    writeBinaryLine(value: { a: number; b: number; c: number }) {
        this.writeInt32(24)
        this.writeFloat64(value.a)
        this.writeFloat64(value.b)
        this.writeFloat64(value.c)
        return this
    }


    private writeLengthPrefixed(writeContent: () => void) {
        this.ensureCapacity(4)
        const lengthOffset = this.offset
        this.writeInt32(0)

        const contentStart = this.offset
        writeContent()
        const length = this.offset - contentStart

        this.buffer.writeInt32BE(length, lengthOffset)
        return this
    }


    writeBinaryArray(
        arr: unknown[],
        elementOid: DataTypeOid,
        elementWriter: (val: any) => void
    ) {
        const hasNull = arr.some(v => v == null)

        this.writeLengthPrefixed(() => {
            this.writeInt32(1)
            this.writeInt32(hasNull ? 1 : 0)
            this.writeInt32(elementOid)
            this.writeInt32(arr.length)
            this.writeInt32(1)

            for (const val of arr) {
                if (val == null) {
                    this.writeNull()
                } else {
                    elementWriter(val)
                }
            }
        })

        return this
    }


    writeBinaryBoolArray(value: (boolean | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Bool, v => this.writeBinaryBool(v))
    }


    writeBinaryInt2Array(value: (number | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Int2, v => this.writeBinaryInt2(v))
    }


    writeBinaryInt4Array(value: (number | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Int4, v => this.writeBinaryInt4(v))
    }


    writeBinaryInt8Array(value: (number | bigint | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Int8, v =>
            typeof v === 'bigint' ? this.writeBinaryBigInt8(v) : this.writeBinaryInt8(v)
        )
    }


    writeBinaryTextArray(value: (string | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Text, v => this.writeBinaryString(v))
    }


    writeBinaryVarcharArray(value: (string | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Varchar, v => this.writeBinaryString(v))
    }


    writeBinaryJsonArray(value: (unknown | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Json, v => this.writeBinaryJson(v))
    }


    writeBinaryJsonbArray(value: (unknown | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Jsonb, v => this.writeBinaryJsonb(v))
    }


    writeBinaryUuidArray(value: (string | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Uuid, v => this.writeBinaryUuid(v))
    }

    
    writeBinaryNumericArray(value: (number | string | null | undefined)[]) {
        return this.writeBinaryArray(value, DataTypeOids.Numeric, v => this.writeBinaryNumeric(v))
    }


    writeNull() {
        this.writeInt32(-1)
        return this
    }


    writeBind(
        portName: string | "",
        meta: StatementMeta,
        params: unknown[],
    ): PostgresError | null {
        this.mark()

        this.startRequest(RequestTypes.Bind)
            .writeCString(portName)
            .writeCString(meta.statement)
            .writeInt16(params.length)

        
        for (let i = 0; i < params.length; i++) {
            this.writeInt16(1)
        }

        this.writeInt16(params.length)

        for (let i = 0; i < params.length; i++) {
            const value = params[i]
            const oid = meta.parameters[i]

            if (value == null) {
                this.writeNull()
                continue
            }
            const handler = handlers[oid]

            if (!handler || !handler.validate(value)) {
                this.rollback()
                return createBindTypeError(i, oid, value)
            }

            handler.write(this, value)
        }

        this
            .writeInt16(1)
            .writeInt16(1)
            .endRequest()

        return null
    }
}
