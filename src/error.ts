import { DataTypeOid, DataTypeOids } from "./protocol/constants"
import { handlers } from "./protocol/types"

export class PostgresError extends Error {
    constructor(
        public override message: string, 
        public code: string = 'undeclared', 
        public detail: string = '', 
        public severity: string = '',
        public where: string = '',
        public hint: string = '',          
        public position: string = '',      
        public dataType: string = '',      
        public constraint: string = ''     
    ) {
        super(message)
        this.name = "PostgresError"
    }

    get isParseError(): boolean {
        return (
            this.code.startsWith('42') || 
            this.code === '26000'         
        )
    }

    get isBindError(): boolean {
        return (
            this.code.startsWith('22') || 
            this.code === '0A000'         
        )
    }

    get isPlanInvalidationError(): boolean {
        return this.code === '0A000' || this.code === '42P05'   
    }

    get shouldInvalidateStatementCache(): boolean {
        return this.isParseError || this.isBindError || this.isPlanInvalidationError
    }

    get isConstraintViolation(): boolean {
        return this.code.startsWith('23')
    }

    get isDeadlock(): boolean {
        return this.code === '40P01'
    }

    get isTimeout(): boolean {
        return this.code === '57014'
    }

    get isConnectionFailure(): boolean {
        return this.code.startsWith('08') || (this.code.startsWith('57') && this.code !== '57014')
    }
}


export const ErrNonceMismatch = new PostgresError("Protocol violation: server nonce doesn't match client nonce")
export const ErrPasswordRequired = new PostgresError('The authorization method requires a password.')
export const ErrSocketFailedDuringAuth = new PostgresError("Socket failed during auth")

export const ErrQueryTimeout = new PostgresError('Query timeout', '57014')

export const ErrPoolClosed = new PostgresError("Pool is closed", 'pool_closed', '', "ERROR")

export const ErrTransactionClosed = new PostgresError("Transaction closed", "transaction_closed", '', "ERROR")

export const ErrConnectionClosed = new PostgresError("Connection is closed", 'connection_closed', "", "ERROR")
export const ErrConnectionReconnecting = new PostgresError("Connection are reconnecting", "connection_reconnecting", "", "ERROR")
export const ErrSSLDenied = new PostgresError("SSL is required but server denied it")
export const ErrDatabaseNotFound = new PostgresError('Database with that dsn not found')
export const ErrUntrustedCertificate = new PostgresError("Database SSL certificate is untrusted or self-signed")
export const ErrCertificateFileNotFound = new PostgresError("The SSL certificate file specified in caPath was not found")


function describeValue(value: unknown): string {
    if (value === null) return 'null'
    if (value === undefined) return 'undefined'
    if (typeof value === 'bigint') return `${value}n`
    if (typeof value === 'string') {
        return value.length > 60 ? `${JSON.stringify(value.slice(0, 60))}…` : JSON.stringify(value)
    }
    if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
        return `Uint8Array(length=${value.length})`
    }
    if (value instanceof Date) {
        return `Date(${isNaN(value.getTime()) ? 'Invalid Date' : value.toISOString()})`
    }
    if (Array.isArray(value)) {
        const preview = value.slice(0, 5).map(describeValue).join(', ')
        return `[${preview}${value.length > 5 ? ', …' : ''}] (length=${value.length})`
    }
    if (typeof value === 'object') {
        try {
            const json = JSON.stringify(value)
            return json.length > 120 ? `${json.slice(0, 120)}…` : json
        } catch {
            return Object.prototype.toString.call(value)
        }
    }
    return String(value)
}


export function createBindTypeError(index: number, expectedType: DataTypeOid, actualValue: unknown): PostgresError {
    const position = index + 1
    const info = handlers[expectedType]

    const expectedPgType = info?.pgType ?? `oid ${expectedType}`
    const expectedJsShape = info?.jsShape ?? '(тип не описан в справочнике)'

    const message =
        `Bind error: Parameter $${position} type mismatch. ` +
        `Expected PostgreSQL type "${expectedPgType}" (${expectedJsShape}), received: ${describeValue(actualValue)}.`

    return new PostgresError(
        message,
        '22000',
        `Parameter $${position} failed client-side binary validation for pg type "${expectedPgType}".`,
        'ERROR',
        'writeBind() inside driver',
        `Pass a value matching ${expectedJsShape} for $${position}.`,
        '',
        expectedPgType,
    )
}