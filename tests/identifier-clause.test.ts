import { describe, it } from "node:test"
import { deepEqual as assert, throws } from "node:assert"
import { sql } from "../src"

describe("identifier clause test", () => {
    const createParams = () => ({
        text: "",
        args: [] as any[],
    })

    it("ident test", () => {
        const params = createParams()
        
        sql.ident("identificator").mapIntoQuery(params)

        assert(params.args.length, 0)
        assert(params.text, '"identificator"')
    })
})
