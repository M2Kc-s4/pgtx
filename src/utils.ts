import { Clause } from "./clauses/abstract.clause"
import { ClauseStrategyParams, QueryText } from "./types"

const cache = new WeakMap<TemplateStringsArray, string>()

export function compileSqlTemplate(templates: TemplateStringsArray, args: unknown[], argOffset = 0) {
    const cached = cache.get(templates)
    
    if (cached) return {args, text: cached as QueryText}
    
    const templateLength = templates.length
    
    const query: ClauseStrategyParams = {
        text: '',
        args: [] as unknown[],
    }

    templates.forEach((template, index) => {
        query.text += template

        if (index === templateLength - 1) return

        const value = args[index]

        if (value instanceof Clause) {
            value.mapIntoQuery(query)
        } else {
            query.args.push(value)
            query.text += `$${query.args.length + argOffset}`
        }
    })

    !args.some(value => value instanceof Clause) && cache.set(templates, query.text)
    
    return query as {text: QueryText, args: unknown[]}
}
