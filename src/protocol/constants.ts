import { ValueOF } from "../types"

export const RequestTypes = {
    SimpleQuery: 81, // "Q"
    Parse: 80,       // "P"
    Bind: 66,        // "B"
    Execute: 69,     // "E"
    Sync: 83,        // "S"
    Password: 112,   // "p"
    Describe: 68,    // "D"
    Close: 67        // "C"
} as const

export type RequestType = ValueOF<typeof RequestTypes>


export const ResponseTypes = {
    RowDescription: 84,       // "T"
    DataRow: 68,              // "D"
    ComandComplete: 67,       // "C"
    ReadyForQuery: 90,        // "Z"
    ErrorResponse: 69,        // "E"
    NoticeResponse: 78,       // "N"
    Authentication: 82,       // "R"
    BackendKeyData: 75,       // "K"
    ParamaterStatus: 83,      // "S"
    ParseComplete: 49,        // "1"
    BindComplete: 50,         // "2"
    CloseComplete: 51,        // "3"
    ParameterDescription: 116, // "t"
    NoData: 110,              // "n"
    Notice: 78,               // "N"
    NotificationResponse: 65, // "A"
    SSLOk: 83,                // "S"
    SSLDenied: 78             // "N"
} as const

export type ResponseType = ValueOF<typeof ResponseTypes>


export const DataTypeOids = {
    Bool: 16, // parameter
    Bytea: 17, 

    Char: 18, // parameter
    Name: 19,
    Text: 25, // parameter
    Varchar: 1043, // parameter
    Bpchar: 1042, // parameter

    Int2: 21, // parameter
    Int4: 23,// parameter
    Int8: 20, // parameter

    Float4: 700, 
    Float8: 701, 
    Numeric: 1700, 

    Timestamp: 1114,    
    Timestamptz: 1184,  
    Date: 1082,
    Time: 1083,
    Timetz: 1266,
    Interval: 1186,

    Json: 114,
    Jsonb: 3802,

    Uuid: 2950,
    Cidr: 650,
    Inet: 869,
    Macaddr: 829,

    Oid: 26,
    Xid: 28,
    Cid: 29,
    Regproc: 24,

    Point: 600,
    Lseg: 601,
    Path: 602,
    Box: 603,
    Polygon: 604,
    Line: 628,

    BoolArray: 1000,
    Int2Array: 1005,
    Int4Array: 1007,
    Int8Array: 1016,
    TextArray: 1009,
    VarcharArray: 1015,
    JsonArray: 199,
    JsonbArray: 3807,
    UuidArray: 2951,
    NumericArray: 1231
} as const

export type DataTypeOid = typeof DataTypeOids[keyof typeof DataTypeOids];

export const INT4Length = 4


export const AuthenticationCodes = {
    Ok: 0,
    CleartextPassword: 3,
    MD5Password: 5,
    SASL: 10,
    SASLContinue: 11,
    SASLFinal: 12,
} as const

export type AuthenticationCode = ValueOF<typeof AuthenticationCodes>


export const TransactionStatuses = {
    Idle: 73,                  // "I"
    InTransactionBlock: 84,    // "T"
    FailedTransactionBlock: 69, // "E"
} as const

export type TransactionStatus = ValueOF<typeof TransactionStatuses>


export const DescribeType = {
    Statement: "S",
    Portal: "P",
} as const

export type DescribeType = ValueOF<typeof DescribeType>

export const EMPTY_ARRAY = Object.freeze([]) as never[]
