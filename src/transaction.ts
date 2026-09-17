import { Begin, Future } from "fluent-future"
import { IdentifierClause } from "./clauses"
import { Connection } from "./connection"
import { ErrConnectionClosed, ErrTransactionClosed, ErrTransactionInProgress, PostgresError } from "./error"
import { Row } from "./types"
import { compileSqlTemplate } from "./utils"

/**
 * Represents an active SQL transaction.
 * All queries are executed on a single dedicated connection.
 */
export class Transaction {
    private isFinished: boolean = false

    constructor(
        readonly conn: Connection
    ) {}


    /**
     * Returns true if the transaction is still open (not committed or rolled back).
     */
    public get isActive(): boolean {
        return !this.isFinished
    }
    

    /**
     * Commits the current transaction.
     */
    public commit() {
        return this.execute`COMMIT`
            .tap(() => this.isFinished = true)
    }


    /**
     * Rolls back the current transaction.
     */
    public rollback() {
        return this.execute`ROLLBACK` 
            .tap(() => this.isFinished = true)
    }
    

    /**
     * Executes a query within the current transaction.
     */
    query<T extends Row>(templates: TemplateStringsArray, ...params: any[]) {
        if (this.isFinished) {
            this.conn['_logError'](ErrTransactionClosed)
            return Future.reject(ErrTransactionClosed)
        }
        
        return this.conn.query<T>(templates, ...params)
    }


    /**
     * Like {@link query}, but for statements that don't return rows (INSERT/UPDATE/DDL/etc).
     *
     * @example
     * await tx.execute`UPDATE users SET name = ${name} WHERE id = ${id}`
     */
    execute(templates: TemplateStringsArray, ...params: any[]) {
        if (this.isFinished) {
            this.conn['_logError'](ErrTransactionClosed)
            return Future.reject(ErrTransactionClosed)
        }
        
        return this.conn.execute(templates, ...params)
    }


    /**
     * Streams query results as a `ReadableStream`, without buffering rows in memory.
     * Ideal for large result sets or piping straight into an HTTP response.
     *
     * @example
     * for await (const row of conn.stream<User>`SELECT * FROM orders`) { ... }
     */
    stream<T extends Row>(templates: TemplateStringsArray, ...params: any[]) {
        if (this.isFinished) {
            this.conn['_logError'](ErrTransactionClosed)
            throw ErrTransactionClosed
        }

        return this.conn.stream<T>(templates, ...params)
    }


    /** Sends a `pg_notify` message on `channelName` (payload ≤ 8000 bytes). */
    notify(channelName: string, payload: string = "") {
        return this.conn.notify(channelName, payload)
    }


    /**
     * Creates a sub-transaction using PostgreSQL SAVEPOINT.
     * If the callback throws, only the actions within this savepoint are rolled back.
     * 
     * @example
     * await tx.savepoint('my_point', async (stx) => {
     *   await stx.query`INSERT ...`;
     *   if (error) throw new Error() // Only this insert rolls back
     * });
     */
    public savepoint<T>(name: string, callback: (tx: Transaction) => Promise<T>) {        
        return Begin<PostgresError>()
            .andThen(() => this.query`SAVEPOINT ${IdentifierClause.create(name)}`)
            .andThen(() =>
                Future.of(() => callback(this))
                    .tap(() => this.query`RELEASE SAVEPOINT ${IdentifierClause.create(name)}`)
                    .tapErr(() => this.query`ROLLBACK TO SAVEPOINT ${IdentifierClause.create(name)}`)
            )
    }
}
