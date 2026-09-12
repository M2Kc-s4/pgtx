import { ClauseStrategyParams } from "../types";
import { Clause} from "./abstract.clause";

export class LiteralClause<T extends string> extends Clause {
    private constructor(
        readonly value: T
    ) {
        super()
    }

    static create<T extends string>(value: T): LiteralClause<T> {
        return new LiteralClause(value)
    }

    override mapIntoQuery(params: ClauseStrategyParams) {
        params.text += this.value
    }
}