import { describe, it } from "node:test"
import { deepEqual as assert, throws } from "node:assert"
import { sql } from "../src"

describe("literal clause test", () => {
    const createParams = () => ({
        text: "",
        args: [] as any[],
    })

    it('literal test', () => {
        const params = createParams()
        
        sql.literal("literal").mapIntoQuery(params)

        assert(params.text, "literal")
        assert(params.args.length, 0)
    })
})