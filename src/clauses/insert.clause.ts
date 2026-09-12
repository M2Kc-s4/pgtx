import { Clause } from "./abstract.clause";
import { ClauseStrategyParams } from "../types";

export class InsertClause<T extends Record<string, any>> extends Clause {
    private constructor(
        readonly inserts: T[]
    ) {super()}

    static create<T extends Record<string, any>>(...objects: NoInfer<T>[]) {
        return new InsertClause<T>(objects)
    }

    override mapIntoQuery(params: ClauseStrategyParams) {
        if (this.inserts.length === 0) {
            params.args.push(undefined)
            params.text += `(undefined) values ($${params.args.length})`
            return
        }

        const columns = Object.keys(this.inserts[0])
        const columnsCount = columns.length

        params.text += `(${columns.join(', ')}) VALUES `

        this.inserts.forEach((object, index) => {
            if (index) params.text += ', '
            

            params.text += 
                `(${Object.values(object).map(value => {
                    if (value === undefined) return "DEFAULT"
                    
                    params.args.push(value)
                    return `$${params.args.length}`
                })
                    .join(", ")})`
        })
    }
}