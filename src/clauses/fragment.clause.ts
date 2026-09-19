import { Clause,  } from "./abstract.clause";
import { compileSqlTemplate } from "../utils";
import { ClauseStrategyParams } from "../types";

export class FragmentClause extends Clause {
    private constructor(
        readonly templates: TemplateStringsArray,
        readonly args: any[]
    ) {super()}

    static create(strings: TemplateStringsArray, ...values: any[]) {
        return new FragmentClause(strings, values)
    }

    override mapIntoQuery(params: ClauseStrategyParams) {
        const compiled = compileSqlTemplate(this.templates, this.args, params.args.length)

        params.args.push(...compiled.args)
        params.text += compiled.text
    }
}