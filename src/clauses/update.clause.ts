import { ClauseStrategyParams } from "../types";
import { Clause } from "./abstract.clause";

export class UpdateClause<T extends Record<string, any>> extends Clause {
    private constructor(
        readonly updateMap: T,
    ) { super() }

    static create<T extends Record<string, any>>(object: T) {
        return new UpdateClause<T>(object)
    }

    override mapIntoQuery(params: ClauseStrategyParams) {
        const entries = Object.entries(this.updateMap).filter(([_, value]) => value !== undefined)
            
        entries.forEach(([key, value], index) => {
            if (index) params.text += ', '
            
            params.args.push(value)
            params.text += `${key} = $${params.args.length}`
        })
    }
}