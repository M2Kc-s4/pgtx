import { Clause } from "./clauses/abstract.clause"
import { PostgresError } from "./error"
import { ClauseStrategyParams,  } from "./types"

const cache = new WeakMap<TemplateStringsArray, string>()

export function compileSqlTemplate(templates: TemplateStringsArray, args: unknown[], argOffset = 0) {
    const cached = cache.get(templates)
    
    if (cached) return {args, text: cached}
    
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
    
    return query 
}

export function logQuery(text: string, args: unknown[]) {
    console.log(
        `\n\x1b[36m┌─ QUERY ─────────────────────────────────────────\x1b[0m\n`
        + `\x1b[36m│\x1b[0m ${text}\n` 
        + `${args.length !== 0 ? `\x1b[36m│\x1b[0m \x1b[90mArguments:\x1b[0m [${args}]\n` : ''}` 
        + `\x1b[36m└────────────────────────────────────────────────\x1b[0m` 
    )
}


export function logNotice(notice: PostgresError) {
    console.log( 
        `\n\x1b[33m┌─ NOTICE ───────────────────────────────────────\x1b[0m\n` 
        + `\x1b[33m│\x1b[0m ${notice}\n` 
        + `\x1b[33m└────────────────────────────────────────────────\x1b[0m\n` 
    ) 
}


export function logError(error: PostgresError) {
    console.log( 
        `\n\x1b[31m┌─ ERROR ────────────────────────────────────────\x1b[0m\n` 
        + `\x1b[31m│\x1b[0m ${error}\n` 
        + `\x1b[31m└────────────────────────────────────────────────\x1b[0m\n` 
    ) 
}

export function safe<A extends unknown[]>(cb: (...a: A) => void, ...a: A) {
    try { cb(...a) } catch (e) { queueMicrotask(() => { throw e }) }
}