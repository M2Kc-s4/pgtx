import { Future, Resolvers } from "fluent-future";
import { ColumnDescription, QueryText, Row, StatementMeta, StatementName } from "./types";
import { ErrQueryTimeout, PostgresError } from "./error";


export abstract class Query {
    protected _timer?: NodeJS.Timeout
    
    constructor(timeout: number) {
        this._timer = setTimeout(() => {
            this.error(ErrQueryTimeout)
        }, timeout)
    }


    abstract error(cause: PostgresError): void

    abstract complete(...args: any[]): void
}


export class CollectQuery<T extends Row> extends Query {
    private _rows: T[] = []

    constructor(
        public meta: StatementMeta,
        public text: QueryText,
        public args: unknown[],
        public columns: ColumnDescription[] | null,
        public resolvers: Resolvers<Future<T[], PostgresError>>,
        timeout: number,
    )  {
        super(timeout)
    }    


    push(value: T) {
        this._rows.push(value)
    }

    
    error(cause: PostgresError) {
        clearTimeout(this._timer)
        this.resolvers.reject(cause)
    }


    complete() {
        clearTimeout(this._timer)
        this.resolvers.resolve(this._rows)
    }
}


export class ExecuteQuery extends Query {
    constructor(
        public meta: StatementMeta,
        public text: QueryText,
        public args: unknown[],
        public resolvers: Resolvers<Future<void, PostgresError>>,
        timeout: number,
    ) {
        super(timeout)
    }

    error(cause: PostgresError) {
        clearTimeout(this._timer)
        this.resolvers.reject(cause)
    }


    complete() {
        clearTimeout(this._timer)
        this.resolvers.resolve()
    }
}


export class StreamQuery<T> extends Query {
    constructor(
        public meta: StatementMeta,
        public text: QueryText,
        public args: unknown[],
        public controller: ReadableStreamDefaultController<T>,
        public columns: ColumnDescription[] | null,
        timeout: number
    ) {
        super(timeout)
    }


    push(value: T) {
        try {
            this.controller.enqueue(value)
        } catch {}
    }


    error(cause: PostgresError) {
        clearTimeout(this._timer)
        this.controller.error(cause)
    }


    complete() {
        clearTimeout(this._timer)
        try {
            this.controller.close()
        } catch {}
    }
}

export class ParseQuery extends Query {
    constructor(
        public meta: StatementMeta,
        public text: QueryText,
        public resolvers: Resolvers<Future<StatementMeta, PostgresError>>,
        timeout: number
    )  {
        super(timeout)
    }    
    
    error(cause: PostgresError) {
        clearTimeout(this._timer)
        this.resolvers.reject(cause)
    }


    complete() {
        clearTimeout(this._timer)
        this.resolvers.resolve(this.meta)
    }
}

export type PostgresQuery = ParseQuery | CollectQuery<any> | StreamQuery<any> | ExecuteQuery