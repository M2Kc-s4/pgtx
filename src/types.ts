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

export type ConnectionEvent = "error" | "notice" | "notify" | "query" | "close"

export type ErrorHandler = (error: PostgresError) => void
export type NoticeHandler = (notice: PostgresError) => void
export type NotifyHandler = (channel: string, payload: string) => void
export type QueryHandler = (text: string, args: unknown[]) => void
export type CloseHandler = () => void

export type Handlers = {
    error: ErrorHandler
    notice: NoticeHandler
    query: QueryHandler
    notify: NotifyHandler
    close: CloseHandler
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
    ssl: SSLMode
    caPath?: string
}

export type ParameterDescription = DataTypeOid[]

export type StatementMeta = {
    statement: string
    columns: ColumnDescription[],
    parameters: ParameterDescription
}



export type PoolPartialConfig = ConnectionPartialConfig &  {
    max?: number
    defaultHandlers?: Partial<Handlers>
}

export type PoolConfig = ConnectionPartialConfig & {
    max: number
    defaultHandlers?: Partial<Handlers>
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