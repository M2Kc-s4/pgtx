import { Connection } from "./connection"
import { PostgresError } from "./error"
import { DataTypeOid } from "./protocol/constants"

export type Branded<T, Brand> = T & {__brand: Brand}

export type ValueOF<T extends Record<string, unknown>> = T[keyof T]

export type ClauseStrategyParams = {
    text: string,
    args: any[],
}

export type ColumnDescription = {
    name: string
    typeOID: DataTypeOid
}

export type SSLMode = 'disable' | 'prefer' | 'require'

export type LogLevel = "none" | "error" | "notice" | "query"

export type ConnectionPartialConfig = {
    user: string
    password?: string
    host: string
    port: number
    database: string
    logLevel?: LogLevel,
    int8toBigint?: boolean,
    queryTimeout?: number
    syncSсhedule?: "beforeMicrotask" | "afterMicrotask" | "Immediate",
    ssl?: SSLMode
    caPath?: string
}


export type ConnectionConfig = {
    user: string
    password?: string
    host: string
    port: number
    database: string
    logLevel: LogLevel,
    int8toBigint: boolean,
    queryTimeout: number
    syncSсhedule: "beforeMicrotask" | "afterMicrotask" | "Immediate"
    ssl: SSLMode
    caPath?: string
}

export type ConnectorConfig = Pick<ConnectionConfig, 'syncSсhedule'>


export type StatementName = Branded<string, 'StatementName'>

export type QueryText = Branded<string, 'QueryText'>

export type ChannelName = Branded<string, "ChannelName">

export type ParameterDescription = DataTypeOid[]

export type StatementMeta = {
    statement: StatementName
    columns: ColumnDescription[],
    parameters: ParameterDescription
}


export type PoolPartialConfig = ConnectionPartialConfig & {
    max?: number
}

export type PoolConfig = ConnectionPartialConfig & {
    max: number
}


export type Waiter = {
    resolve: (conn: Connection) => void
    reject: (err: PostgresError) => void
}


export type Row = Record<string, any>


export type PgPoint = { x: number, y: number }
export type PgLine = { a: number, b: number, c: number }
export type PgLineSegment = { a: PgPoint, b: PgPoint }
export type PgBox = { high: PgPoint, low: PgPoint }
export type PgPath = { closed: boolean, points: PgPoint[] }
export type PgPolygon = { points: PgPoint[] }
export type PgInterval = { months: number, days: number, microseconds: number }