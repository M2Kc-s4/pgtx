import { Clause } from "./clauses/abstract.clause"
import { PostgresError } from "./error"
import { CollectQuery, ExecuteQuery, PostgresQuery, StreamQuery } from "./query"
import { ClauseStrategyParams, LogLevel, QueryText } from "./types"

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

export function logQuery(query: StreamQuery<any> | CollectQuery<any> | ExecuteQuery, logLevel: LogLevel) {
    if (logLevel === 'query') { 
        console.log(
            `\n\x1b[36m┌─ QUERY ─────────────────────────────────────────\x1b[0m\n`
            + `\x1b[36m│\x1b[0m ${query.text}\n` 
            + `${query.args.length !== 0 ? `\x1b[36m│\x1b[0m \x1b[90mArguments:\x1b[0m [${query.args}]\n` : ''}` 
            + `\x1b[36m└────────────────────────────────────────────────\x1b[0m` 
        ) 
    }
}


export function logNotice(notice: PostgresError, logLevel: LogLevel) {
    if (logLevel === 'notice' || logLevel === 'query') { 
        console.log( 
            `\n\x1b[33m┌─ NOTICE ───────────────────────────────────────\x1b[0m\n` 
            + `\x1b[33m│\x1b[0m ${notice}\n` 
            + `\x1b[33m└────────────────────────────────────────────────\x1b[0m\n` 
        ) 
    }
}


export function logError(error: PostgresError, logLevel: LogLevel) {
    if (logLevel === 'error' || logLevel === 'notice' || logLevel === 'query') { 
        console.log( 
            `\n\x1b[31m┌─ ERROR ────────────────────────────────────────\x1b[0m\n` 
            + `\x1b[31m│\x1b[0m ${error}\n` 
            + `\x1b[31m└────────────────────────────────────────────────\x1b[0m\n` 
        ) 
    }
}