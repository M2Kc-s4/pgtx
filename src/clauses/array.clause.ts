import { ClauseStrategyParams } from "../types";
import { Clause } from "./abstract.clause";

export class ArrayClause extends Clause {
    private constructor(
        private readonly array: any[],
        private readonly separator: string = ", "
    ) { super() }

    static create(array: any[], separator: string = ", "): ArrayClause {

        return new ArrayClause(array, separator) 
    }

    override mapIntoQuery(params: ClauseStrategyParams) {
        this.array.forEach((value, index) => {
            if (index) params.text += this.separator

            if (value instanceof Clause) {
                value.mapIntoQuery(params)
            } 
            else {
                params.args.push(value)
                params.text += `$${params.args.length}`
            }
        })
    }
}