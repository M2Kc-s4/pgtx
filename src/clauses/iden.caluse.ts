import { ClauseStrategyParams } from "../types"
import { Clause } from "./abstract.clause"

export class IdentifierClause<T extends string> extends Clause {
    private constructor(
        readonly value: T
    ) {super()}

    static create<T extends string>(identificator: T) {
        return new IdentifierClause<T>(identificator)
    }

    override mapIntoQuery(params: ClauseStrategyParams) {
        params.text += `"${this.value}"`
    }
}