import { DataTypeOid, DataTypeOids } from "./protocol/constants"

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

export function createBindTypeError(index: number, expectedType: DataTypeOid, actualValue: unknown): PostgresError {
    const position = (index + 1).toString()
    const actualType = actualValue === null ? 'null' : typeof actualValue
    
    const message = `Bind error: Parameter $${position} type mismatch. Expected ${expectedType}, received "${actualType}(${actualValue})".`
    
    return new PostgresError(
        message,
        '22000',                                            // code
        `The parameter at position $${position} failed client-side binary validation.`, // detail
        'ERROR',                                            // severity
        'writeBind() inside driver',                        // where
        `Ensure that the argument passed as $${position} matches the PostgreSQL schema requirements.` // hint
    )
}