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
    public query<T extends Row>(templates: TemplateStringsArray, ...params: any[]) {
        if (this.isFinished) {
            this.conn['_logError'](ErrTransactionClosed)
            return Future.reject(ErrTransactionClosed)
        }

        if (this.conn.isClosed) {
            this.conn['_logError'](ErrConnectionClosed)
            return Future.reject(ErrConnectionClosed)
        }
        
        const {text, args} = compileSqlTemplate(templates, params)

        const resolvers = Future.withResolvers<T[], PostgresError>()
        
        if (this.conn['_reconnecting']) {
            return this.conn['_reconnecting']
                .tapErr(err => this.conn['_logError'](err))
                .andThen(() => this.conn['_performQuery'](text, args, resolvers))
        }

        return this.conn['_performQuery'](text, args, resolvers)
    }


    /**
     * Like {@link query}, but for statements that don't return rows (INSERT/UPDATE/DDL/etc).
     *
     * @example
     * await tx.execute`UPDATE users SET name = ${name} WHERE id = ${id}`
     */
    public execute(templates: TemplateStringsArray, ...params: any[]) {
        if (this.isFinished) {
            this.conn['_logError'](ErrTransactionClosed)
            return Future.reject(ErrTransactionClosed)
        }

        if (this.conn.isClosed) {
            this.conn['_logError'](ErrConnectionClosed)
            return Future.reject(ErrConnectionClosed)
        }

        const {text, args} = compileSqlTemplate(templates, params)

        const resolvers = Future.withResolvers<void, PostgresError>()
        
        if (this.conn['_reconnecting']) {
            return this.conn['_reconnecting']
                .tapErr(err => this.conn['_logError'](err))
                .andThen(() => this.conn['_performExecute'](text, args, resolvers))
        }

        return this.conn['_performExecute'](text, args, resolvers)
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
        if (this.conn.isClosed) {
            this.conn['_logError'](ErrConnectionClosed)
            throw ErrConnectionClosed
        }

        const {text, args} = compileSqlTemplate(templates, params)
        
        let controller!: ReadableStreamDefaultController<T>

        const stream = new ReadableStream<T>({
            start: c => {
                controller = c
            }
        })

        if (this.conn['_reconnecting']) {
            this.conn['_reconnecting']
                .tap(() => this.conn['_performStream']<T>(text, args, controller))
                .tapErr(err => {
                    this.conn['_logError'](err)
                    controller.error(err)
                })
                .recover()

            return stream
        }

        this.conn['_performStream']<T>(text, args, controller)

        return stream
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
