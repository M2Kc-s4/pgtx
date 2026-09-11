import { ConnectionRequestBuffer } from "./connection-request-writer"
import { ConnectionResponseBuffer } from "./connection-response-reader"
import { DataTypeOid, DataTypeOids } from "./constants"

type BindValidator = (value: unknown) => boolean
type BindWriter = (request: ConnectionRequestBuffer, value: any) => void
type FieldReader = (
    response: ConnectionResponseBuffer,
    fieldLength: number,
    int8toBigint: boolean
) => unknown

export interface BindHandler {
    pgType: string
    jsShape: string
    validate: BindValidator
    write: BindWriter
    read: FieldReader
}

const isPoint = (value: unknown): value is { x: number; y: number } =>
    value !== null &&
    typeof value === 'object' &&
    'x' in value && typeof (value as any).x === 'number' &&
    'y' in value && typeof (value as any).y === 'number'


export const handlers: Partial<Record<DataTypeOid, BindHandler>> = {
    [DataTypeOids.Bool]: {
        pgType: 'bool',
        jsShape: 'boolean',
        validate: (v): v is boolean => typeof v === 'boolean',
        write: (req, v) => req.writeBinaryBool(v),
        read: (res) => res.readBool(),
    },


    [DataTypeOids.Text]: {
        pgType: 'text',
        jsShape: 'string',
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Varchar]: {
        pgType: 'varchar',
        jsShape: 'string',
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Char]: {
        pgType: 'char',
        jsShape: 'string',
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Bpchar]: {
        pgType: 'bpchar',
        jsShape: 'string',
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Name]: {
        pgType: 'name',
        jsShape: 'string',
        validate: (v): v is string =>
            typeof v === 'string' && Buffer.byteLength(v, 'utf-8') <= 63,
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Int2]: {
        pgType: 'int2',
        jsShape: 'number (-32768..32767)',
        validate: (v): v is number =>
            typeof v === 'number' &&
            Number.isInteger(v) &&
            v >= -32768 &&
            v <= 32767,
        write: (req, v) => req.writeBinaryInt2(v),
        read: (res) => res.readInt16(),
    },


    [DataTypeOids.Int4]: {
        pgType: 'int4',
        jsShape: 'number (-2147483648..2147483647)',
        validate: (v): v is number =>
            typeof v === 'number' &&
            Number.isInteger(v) &&
            v >= -2147483648 &&
            v <= 2147483647,
        write: (req, v) => req.writeBinaryInt4(v),
        read: (res) => res.readInt32(),
    },


    [DataTypeOids.Int8]: {
        pgType: 'int8',
        jsShape: 'bigint | number (safe integer)',
        validate: (v): v is number | bigint =>
            typeof v === 'bigint' ||
            (typeof v === 'number' && Number.isSafeInteger(v)),
        write: (req, v) =>
            typeof v === 'bigint'
                ? req.writeBinaryBigInt8(v)
                : req.writeBinaryInt8(v as number),
        read: (res, _len, int8toBigint) =>
            int8toBigint ? res.readBigInt64() : res.readInt64(),
    },


    [DataTypeOids.Float4]: {
        pgType: 'float4',
        jsShape: 'number',
        validate: (v): v is number =>
            typeof v === 'number' && Number.isFinite(v),
        write: (req, v) => req.writeBinaryFloat4(v),
        read: (res) => res.readFloat32(),
    },


    [DataTypeOids.Float8]: {
        pgType: 'float8',
        jsShape: 'number',
        validate: (v): v is number =>
            typeof v === 'number' && Number.isFinite(v),
        write: (req, v) => req.writeBinaryFloat8(v),
        read: (res) => res.readFloat64(),
    },


    [DataTypeOids.Bytea]: {
        pgType: 'bytea',
        jsShape: 'Buffer | Uint8Array',
        validate: (v): v is Uint8Array =>
            Buffer.isBuffer(v) || v instanceof Uint8Array,
        write: (req, v) => req.writeBinaryBytea(v),
        read: (res, len) => res.readBytes(len),
    },


    [DataTypeOids.Timestamp]: {
        pgType: 'timestamp',
        jsShape: 'Date',
        validate: (v): v is Date => v instanceof Date,
        write: (req, v) => req.writeBinaryTimestamp(v),
        read: (res) => res.readBinaryTimestamp(),
    },


    [DataTypeOids.Timestamptz]: {
        pgType: 'timestamptz',
        jsShape: 'Date',
        validate: (v): v is Date => v instanceof Date,
        write: (req, v) => req.writeBinaryTimestamp(v),
        read: (res) => res.readBinaryTimestamp(),
    },


    [DataTypeOids.Point]: {
        pgType: 'point',
        jsShape: '{ x: number, y: number }',
        validate: isPoint,
        write: (req, v) => req.writeBinaryPoint(v),
        read: (res) => res.readBinaryPoint(),
    },


    [DataTypeOids.Numeric]: {
        pgType: 'numeric',
        jsShape: 'string ("[-]digits[.digits]")',
        validate: (v): v is string =>
            (typeof v === 'string' && /^-?\d*\.?\d+$/.test(v)),
        write: (req, v) => req.writeBinaryNumeric(v),
        read: (res) => res.readBinaryNumeric(),
    },


    [DataTypeOids.Date]: {
        pgType: 'date',
        jsShape: 'Date',
        validate: (v): v is Date => v instanceof Date,
        write: (req, v) => req.writeBinaryDate(v),
        read: (res) => res.readBinaryDate(),
    },


    [DataTypeOids.Time]: {
        pgType: 'time',
        jsShape: 'string ("HH:MM:SS.mmm")',
        validate: (v): v is string =>
            typeof v === 'string' &&
            /^(\d{2}):(\d{2}):(\d{2})\.(\d{3})$/.test(v),
        write: (req, v) => req.writeBinaryTime(v),
        read: (res) => res.readBinaryTime(),
    },


    [DataTypeOids.Timetz]: {
        pgType: 'timetz',
        jsShape: 'string ("HH:MM:SS.mmm±HH:MM")',
        validate: (v): v is string =>
            typeof v === 'string' &&
            /^(\d{2}):(\d{2}):(\d{2})\.(\d{3})([+-])(\d{2}):(\d{2})$/.test(v),
        write: (req, v) => req.writeBinaryTimetz(v),
        read: (res) => res.readBinaryTimetz(),
    },


    [DataTypeOids.Interval]: {
        pgType: 'interval',
        jsShape: '{ months: number, days: number, microseconds: number }',
        validate: (v): v is { months: number; days: number; microseconds: number } =>
            v !== null &&
            typeof v === 'object' &&
            'months' in v && typeof (v as any).months === 'number' &&
            'days' in v && typeof (v as any).days === 'number' &&
            'microseconds' in v &&
            typeof (v as any).microseconds === 'number',
        write: (req, v) => req.writeBinaryInterval(v),
        read: (res) => res.readBinaryInterval(),
    },


    [DataTypeOids.Json]: {
        pgType: 'json',
        jsShape: 'unknown (JSON-serializable)',
        validate: (_v): _v is unknown => true,
        write: (req, v) => req.writeBinaryJson(v),
        read: (res, len) => res.readBinaryJson(len),
    },


    [DataTypeOids.Jsonb]: {
        pgType: 'jsonb',
        jsShape: 'unknown (JSON-serializable)',
        validate: (_v): _v is unknown => true,
        write: (req, v) => req.writeBinaryJsonb(v),
        read: (res, len) => res.readBinaryJsonb(len),
    },


    [DataTypeOids.Uuid]: {
        pgType: 'uuid',
        jsShape: 'string ("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")',
        validate: (v): v is string =>
            typeof v === 'string' &&
            /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v),
        write: (req, v) => req.writeBinaryUuid(v),
        read: (res) => res.readBinaryUuid(),
    },


    [DataTypeOids.Cidr]: {
        pgType: 'cidr',
        jsShape: 'string (IPv4/IPv6 address or CIDR)',
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryCidr(v),
        read: (res, len) => res.readBinaryInet(len),
    },


    [DataTypeOids.Inet]: {
        pgType: 'inet',
        jsShape: 'string (IPv4/IPv6 address or CIDR)',
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryInet(v),
        read: (res, len) => res.readBinaryInet(len),
    },


    [DataTypeOids.Macaddr]: {
        pgType: 'macaddr',
        jsShape: 'string ("xx:xx:xx:xx:xx:xx")',
        validate: (v): v is string =>
            typeof v === 'string' &&
            /^([0-9a-f]{2}:){5}[0-9a-f]{2}$/i.test(v),
        write: (req, v) => req.writeBinaryMacaddr(v),
        read: (res) => res.readBinaryMacaddr(),
    },


    [DataTypeOids.Oid]: {
        pgType: 'oid',
        jsShape: 'number',
        validate: (v): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0,
        write: (req, v) => req.writeBinaryOid(v),
        read: (res) => res.readBinaryOid(),
    },


    [DataTypeOids.Xid]: {
        pgType: 'xid',
        jsShape: 'number',
        validate: (v): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0,
        write: (req, v) => req.writeBinaryXid(v),
        read: (res) => res.readBinaryXid(),
    },


    [DataTypeOids.Cid]: {
        pgType: 'cid',
        jsShape: 'number',
        validate: (v): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0,
        write: (req, v) => req.writeBinaryCid(v),
        read: (res) => res.readBinaryCid(),
    },


    [DataTypeOids.Regproc]: {
        pgType: 'regproc',
        jsShape: 'number',
        validate: (v): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0,
        write: (req, v) => req.writeBinaryRegproc(v),
        read: (res) => res.readBinaryRegproc(),
    },


    [DataTypeOids.Lseg]: {
        pgType: 'lseg',
        jsShape: '{ a: Point, b: Point }',
        validate: (v): v is { a: { x: number; y: number }; b: { x: number; y: number } } =>
            v !== null && typeof v === 'object' &&
            isPoint((v as any).a) && isPoint((v as any).b),
        write: (req, v) => req.writeBinaryLseg(v),
        read: (res) => res.readBinaryLseg(),
    },


    [DataTypeOids.Path]: {
        pgType: 'path',
        jsShape: '{ closed: boolean, points: Point[] }',
        validate: (v): v is { closed: boolean; points: { x: number; y: number }[] } =>
            v !== null && typeof v === 'object' &&
            typeof (v as any).closed === 'boolean' &&
            Array.isArray((v as any).points) &&
            (v as any).points.every(isPoint),
        write: (req, v) => req.writeBinaryPath(v),
        read: (res) => res.readBinaryPath(),
    },


    [DataTypeOids.Box]: {
        pgType: 'box',
        jsShape: '{ high: Point, low: Point }',
        validate: (v): v is { high: { x: number; y: number }; low: { x: number; y: number } } =>
            v !== null && typeof v === 'object' &&
            isPoint((v as any).high) && isPoint((v as any).low),
        write: (req, v) => req.writeBinaryBox(v),
        read: (res) => res.readBinaryBox(),
    },


    [DataTypeOids.Polygon]: {
        pgType: 'polygon',
        jsShape: '{ points: Point[] }',
        validate: (v): v is { points: { x: number; y: number }[] } =>
            v !== null && typeof v === 'object' &&
            Array.isArray((v as any).points) &&
            (v as any).points.every(isPoint),
        write: (req, v) => req.writeBinaryPolygon(v),
        read: (res) => res.readBinaryPolygon(),
    },


    [DataTypeOids.Line]: {
        pgType: 'line',
        jsShape: '{ a: number, b: number, c: number }',
        validate: (v): v is { a: number; b: number; c: number } =>
            v !== null && typeof v === 'object' &&
            typeof (v as any).a === 'number' &&
            typeof (v as any).b === 'number' &&
            typeof (v as any).c === 'number',
        write: (req, v) => req.writeBinaryLine(v),
        read: (res) => res.readBinaryLine(),
    },


    [DataTypeOids.BoolArray]: {
        pgType: 'bool[]',
        jsShape: '(boolean | null)[]',
        validate: (v): v is (boolean | null | undefined)[] =>
            Array.isArray(v) && v.every(el => el == null || typeof el === 'boolean'),
        write: (req, v) => req.writeBinaryBoolArray(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readBool()),
    },


    [DataTypeOids.Int2Array]: {
        pgType: 'int2[]',
        jsShape: '(number | null)[]',
        validate: (v): v is (number | null | undefined)[] =>
            Array.isArray(v) && v.every(el =>
                el == null || (typeof el === 'number' && Number.isInteger(el) && el >= -32768 && el <= 32767)
            ),
        write: (req, v) => req.writeBinaryInt2Array(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readInt16()),
    },


    [DataTypeOids.Int4Array]: {
        pgType: 'int4[]',
        jsShape: '(number | null)[]',
        validate: (v): v is (number | null | undefined)[] =>
            Array.isArray(v) && v.every(el =>
                el == null || (typeof el === 'number' && Number.isInteger(el) && el >= -2147483648 && el <= 2147483647)
            ),
        write: (req, v) => req.writeBinaryInt4Array(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readInt32()),
    },


    [DataTypeOids.Int8Array]: {
        pgType: 'int8[]',
        jsShape: '(number | bigint | null)[]',
        validate: (v): v is (number | bigint | null | undefined)[] =>
            Array.isArray(v) && v.every(el =>
                el == null ||
                typeof el === 'bigint' ||
                (typeof el === 'number' && Number.isSafeInteger(el))
            ),
        write: (req, v) => req.writeBinaryInt8Array(v),
        read: (res, len, int8toBigint) =>
            res.readBinaryArray(len, () => int8toBigint ? res.readBigInt64() : res.readInt64()),
    },


    [DataTypeOids.TextArray]: {
        pgType: 'text[]',
        jsShape: '(string | null)[]',
        validate: (v): v is (string | null | undefined)[] =>
            Array.isArray(v) && v.every(el => el == null || typeof el === 'string'),
        write: (req, v) => req.writeBinaryTextArray(v),
        read: (res, len) => res.readBinaryArray(len, (l) => res.readRawString(l)),
    },


    [DataTypeOids.VarcharArray]: {
        pgType: 'varchar[]',
        jsShape: '(string | null)[]',
        validate: (v): v is (string | null | undefined)[] =>
            Array.isArray(v) && v.every(el => el == null || typeof el === 'string'),
        write: (req, v) => req.writeBinaryVarcharArray(v),
        read: (res, len) => res.readBinaryArray(len, (l) => res.readRawString(l)),
    },


    [DataTypeOids.JsonArray]: {
        pgType: 'json[]',
        jsShape: 'unknown[]',
        validate: (v): v is unknown[] => Array.isArray(v),
        write: (req, v) => req.writeBinaryJsonArray(v),
        read: (res, len) => res.readBinaryArray(len, (l) => res.readBinaryJson(l)),
    },


    [DataTypeOids.JsonbArray]: {
        pgType: 'jsonb[]',
        jsShape: 'unknown[]',
        validate: (v): v is unknown[] => Array.isArray(v),
        write: (req, v) => req.writeBinaryJsonbArray(v),
        read: (res, len) => res.readBinaryArray(len, (l) => res.readBinaryJsonb(l)),
    },


    [DataTypeOids.UuidArray]: {
        pgType: 'uuid[]',
        jsShape: '(string | null)[]',
        validate: (v): v is (string | null | undefined)[] =>
            Array.isArray(v) && v.every(el =>
                el == null || (typeof el === 'string' && /^[0-9a-f-]{36}$/i.test(el))
            ),
        write: (req, v) => req.writeBinaryUuidArray(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readBinaryUuid()),
    },


    [DataTypeOids.NumericArray]: {
        pgType: 'numeric[]',
        jsShape: '(string | null)[]',
        validate: (v): v is (number | string | null | undefined)[] =>
            Array.isArray(v) && v.every(el =>
                el == null ||
                (typeof el === 'number' && Number.isFinite(el)) ||
                (typeof el === 'string' && /^-?\d*\.?\d+$/.test(el))
            ),
        write: (req, v) => req.writeBinaryNumericArray(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readBinaryNumeric()),
    },
}