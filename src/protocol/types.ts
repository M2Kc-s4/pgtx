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

interface BindHandler {
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
        validate: (v): v is boolean => typeof v === 'boolean',
        write: (req, v) => req.writeBinaryBool(v),
        read: (res) => res.readBool(),
    },


    [DataTypeOids.Text]: {
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Varchar]: {
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Char]: {
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Bpchar]: {
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Name]: {
        validate: (v): v is string =>
            typeof v === 'string' && Buffer.byteLength(v, 'utf-8') <= 63,
        write: (req, v) => req.writeBinaryString(v),
        read: (res, len) => res.readRawString(len),
    },


    [DataTypeOids.Int2]: {
        validate: (v): v is number =>
            typeof v === 'number' &&
            Number.isInteger(v) &&
            v >= -32768 &&
            v <= 32767,
        write: (req, v) => req.writeBinaryInt2(v),
        read: (res) => res.readInt16(),
    },


    [DataTypeOids.Int4]: {
        validate: (v): v is number =>
            typeof v === 'number' &&
            Number.isInteger(v) &&
            v >= -2147483648 &&
            v <= 2147483647,
        write: (req, v) => req.writeBinaryInt4(v),
        read: (res) => res.readInt32(),
    },


    [DataTypeOids.Int8]: {
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
        validate: (v): v is number =>
            typeof v === 'number' && Number.isFinite(v),
        write: (req, v) => req.writeBinaryFloat4(v),
        read: (res) => res.readFloat32(),
    },


    [DataTypeOids.Float8]: {
        validate: (v): v is number =>
            typeof v === 'number' && Number.isFinite(v),
        write: (req, v) => req.writeBinaryFloat8(v),
        read: (res) => res.readFloat64(),
    },


    [DataTypeOids.Bytea]: {
        validate: (v): v is Uint8Array =>
            Buffer.isBuffer(v) || v instanceof Uint8Array,
        write: (req, v) => req.writeBinaryBytea(v),
        read: (res, len) => res.readBytes(len),
    },


    [DataTypeOids.Timestamp]: {
        validate: (v): v is Date => v instanceof Date,
        write: (req, v) => req.writeBinaryTimestamp(v),
        read: (res) => res.readBinaryTimestamp(),
    },


    [DataTypeOids.Timestamptz]: {
        validate: (v): v is Date => v instanceof Date,
        write: (req, v) => req.writeBinaryTimestamp(v),
        read: (res) => res.readBinaryTimestamp(),
    },

    
    [DataTypeOids.Point]: {
        validate: isPoint,
        write: (req, v) => req.writeBinaryPoint(v),
        read: (res) => res.readBinaryPoint(),
    },


    [DataTypeOids.Numeric]: {
        validate: (v): v is string =>
            (typeof v === 'string' && /^-?\d*\.?\d+$/.test(v)),
        write: (req, v) => req.writeBinaryNumeric(v),
        read: (res) => res.readBinaryNumeric(),
    },


    [DataTypeOids.Date]: {
        validate: (v): v is Date => v instanceof Date,
        write: (req, v) => req.writeBinaryDate(v),
        read: (res) => res.readBinaryDate(),
    },


    [DataTypeOids.Time]: {
        validate: (v): v is string =>
            typeof v === 'string' &&
            /^(\d{2}):(\d{2}):(\d{2})\.(\d{3})$/.test(v),

        write: (req, v) => req.writeBinaryTime(v),

        read: (res) => res.readBinaryTime(),
    },

    
    [DataTypeOids.Timetz]: {
        validate: (v): v is string =>
            typeof v === 'string' &&
            /^(\d{2}):(\d{2}):(\d{2})\.(\d{3})([+-])(\d{2}):(\d{2})$/.test(v),

        write: (req, v) => req.writeBinaryTimetz(v),

        read: (res) => res.readBinaryTimetz(),
    },


    [DataTypeOids.Interval]: {
        validate: (v): v is { months: number; days: number; microseconds: number | bigint } =>
            v !== null &&
            typeof v === 'object' &&
            'months' in v && typeof (v as any).months === 'number' &&
            'days' in v && typeof (v as any).days === 'number' &&
            'microseconds' in v &&
            (typeof (v as any).microseconds === 'number' || typeof (v as any).microseconds === 'bigint'),
        write: (req, v) => req.writeBinaryInterval(v),
        read: (res) => res.readBinaryInterval(),
    },


    [DataTypeOids.Json]: {
        validate: (_v): _v is unknown => true,
        write: (req, v) => req.writeBinaryJson(v),
        read: (res, len) => res.readBinaryJson(len),
    },


    [DataTypeOids.Jsonb]: {
        validate: (_v): _v is unknown => true,
        write: (req, v) => req.writeBinaryJsonb(v),
        read: (res, len) => res.readBinaryJsonb(len),
    },


    [DataTypeOids.Uuid]: {
        validate: (v): v is string =>
            typeof v === 'string' &&
            /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v),
        write: (req, v) => req.writeBinaryUuid(v),
        read: (res) => res.readBinaryUuid(),
    },


    [DataTypeOids.Cidr]: {
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryCidr(v),
        read: (res, len) => res.readBinaryInet(len),
    },


    [DataTypeOids.Inet]: {
        validate: (v): v is string => typeof v === 'string',
        write: (req, v) => req.writeBinaryInet(v),
        read: (res, len) => res.readBinaryInet(len),
    },


    [DataTypeOids.Macaddr]: {
        validate: (v): v is string =>
            typeof v === 'string' &&
            /^([0-9a-f]{2}:){5}[0-9a-f]{2}$/i.test(v),
        write: (req, v) => req.writeBinaryMacaddr(v),
        read: (res) => res.readBinaryMacaddr(),
    },


    [DataTypeOids.Oid]: {
        validate: (v): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0,
        write: (req, v) => req.writeBinaryOid(v),
        read: (res) => res.readBinaryOid(),
    },


    [DataTypeOids.Xid]: {
        validate: (v): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0,
        write: (req, v) => req.writeBinaryXid(v),
        read: (res) => res.readBinaryXid(),
    },


    [DataTypeOids.Cid]: {
        validate: (v): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0,
        write: (req, v) => req.writeBinaryCid(v),
        read: (res) => res.readBinaryCid(),
    },


    [DataTypeOids.Regproc]: {
        validate: (v): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0,
        write: (req, v) => req.writeBinaryRegproc(v),
        read: (res) => res.readBinaryRegproc(),
    },


    [DataTypeOids.Lseg]: {
        validate: (v): v is { a: { x: number; y: number }; b: { x: number; y: number } } =>
            v !== null && typeof v === 'object' &&
            isPoint((v as any).a) && isPoint((v as any).b),
        write: (req, v) => req.writeBinaryLseg(v),
        read: (res) => res.readBinaryLseg(),
    },


    [DataTypeOids.Path]: {
        validate: (v): v is { closed: boolean; points: { x: number; y: number }[] } =>
            v !== null && typeof v === 'object' &&
            typeof (v as any).closed === 'boolean' &&
            Array.isArray((v as any).points) &&
            (v as any).points.every(isPoint),
        write: (req, v) => req.writeBinaryPath(v),
        read: (res) => res.readBinaryPath(),
    },


    [DataTypeOids.Box]: {
        validate: (v): v is { high: { x: number; y: number }; low: { x: number; y: number } } =>
            v !== null && typeof v === 'object' &&
            isPoint((v as any).high) && isPoint((v as any).low),
        write: (req, v) => req.writeBinaryBox(v),
        read: (res) => res.readBinaryBox(),
    },


    [DataTypeOids.Polygon]: {
        validate: (v): v is { points: { x: number; y: number }[] } =>
            v !== null && typeof v === 'object' &&
            Array.isArray((v as any).points) &&
            (v as any).points.every(isPoint),
        write: (req, v) => req.writeBinaryPolygon(v),
        read: (res) => res.readBinaryPolygon(),
    },


    [DataTypeOids.Line]: {
        validate: (v): v is { a: number; b: number; c: number } =>
            v !== null && typeof v === 'object' &&
            typeof (v as any).a === 'number' &&
            typeof (v as any).b === 'number' &&
            typeof (v as any).c === 'number',
        write: (req, v) => req.writeBinaryLine(v),
        read: (res) => res.readBinaryLine(),
    },



    [DataTypeOids.BoolArray]: {
        validate: (v): v is (boolean | null | undefined)[] =>
            Array.isArray(v) && v.every(el => el == null || typeof el === 'boolean'),
        write: (req, v) => req.writeBinaryBoolArray(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readBool()),
    },


    [DataTypeOids.Int2Array]: {
        validate: (v): v is (number | null | undefined)[] =>
            Array.isArray(v) && v.every(el =>
                el == null || (typeof el === 'number' && Number.isInteger(el) && el >= -32768 && el <= 32767)
            ),
        write: (req, v) => req.writeBinaryInt2Array(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readInt16()),
    },


    [DataTypeOids.Int4Array]: {
        validate: (v): v is (number | null | undefined)[] =>
            Array.isArray(v) && v.every(el =>
                el == null || (typeof el === 'number' && Number.isInteger(el) && el >= -2147483648 && el <= 2147483647)
            ),
        write: (req, v) => req.writeBinaryInt4Array(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readInt32()),
    },


    [DataTypeOids.Int8Array]: {
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
        validate: (v): v is (string | null | undefined)[] =>
            Array.isArray(v) && v.every(el => el == null || typeof el === 'string'),
        write: (req, v) => req.writeBinaryTextArray(v),
        read: (res, len) => res.readBinaryArray(len, (l) => res.readRawString(l)),
    },


    [DataTypeOids.VarcharArray]: {
        validate: (v): v is (string | null | undefined)[] =>
            Array.isArray(v) && v.every(el => el == null || typeof el === 'string'),
        write: (req, v) => req.writeBinaryVarcharArray(v),
        read: (res, len) => res.readBinaryArray(len, (l) => res.readRawString(l)),
    },


    [DataTypeOids.JsonArray]: {
        validate: (v): v is unknown[] => Array.isArray(v),
        write: (req, v) => req.writeBinaryJsonArray(v),
        read: (res, len) => res.readBinaryArray(len, (l) => res.readBinaryJson(l)),
    },


    [DataTypeOids.JsonbArray]: {
        validate: (v): v is unknown[] => Array.isArray(v),
        write: (req, v) => req.writeBinaryJsonbArray(v),
        read: (res, len) => res.readBinaryArray(len, (l) => res.readBinaryJsonb(l)),
    },


    [DataTypeOids.UuidArray]: {
        validate: (v): v is (string | null | undefined)[] =>
            Array.isArray(v) && v.every(el =>
                el == null || (typeof el === 'string' && /^[0-9a-f-]{36}$/i.test(el))
            ),
        write: (req, v) => req.writeBinaryUuidArray(v),
        read: (res, len) => res.readBinaryArray(len, () => res.readBinaryUuid()),
    },


    [DataTypeOids.NumericArray]: {
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