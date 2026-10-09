import { describe, it } from "node:test"
import  { deepEqual as assert, throws } from "node:assert"
import { sql } from "../src"

function createParams() {
    return {
        text: '',
        args: [] as any[],
    }
}

describe('Clauses test', () => {
    describe("Array clause test", () => {

        it("test array of primitive", () => {
            const array = [10, "string", true, null]
            const params = createParams()

            sql.array(array).mapIntoQuery(params)

            assert(params.text, "$1, $2, $3, $4")
            assert(params.args, [10, "string", true, null])
        })

        it("test array of clauses", () => {
            const array = [
                sql.literal("static"), 
                sql.ident("ident"), 
                sql.fragment`sql fragment ${"value"}`
            ]
            const params = createParams()

            sql.array(array).mapIntoQuery(params)

            assert(params.text, `static, "ident", sql fragment $1`)
            assert(params.args, ["value"])
        })

        it("array separator test", () => {
            const array = [
                sql.fragment`name = ${"name"}`, 
                sql.fragment`age = ${18}`, 
                sql.fragment`1 = 1`
            ]
            const params = createParams()

            sql.array(array, " AND ").mapIntoQuery(params)

            assert(params.text, "name = $1 AND age = $2 AND 1 = 1")
            assert(params.args, ["name", 18])
        })
    })

    describe("Insert clause test", () => {

        it("insert test", () => {
            const entities = [{id: 123, name: "Bob"}, {id: 124, name: "Alice"}]
            const params = createParams()

            sql.insert(...entities).mapIntoQuery(params)

            assert(params.text, `(id, name) VALUES ($1, $2), ($3, $4)`)
            assert(params.args, [123, "Bob", 124, "Alice"])
        })        

        it("undefined behavior test (DEFAULT)", () => {
            const entities = [{id: 123, name: "Bob"}, {id: 124, name: undefined}]
            const params = createParams()

            sql.insert(...entities).mapIntoQuery(params)

            assert(params.text, `(id, name) VALUES ($1, $2), ($3, DEFAULT)`)
            assert(params.args, [123, "Bob", 124])
        })
    })

    describe("Fragment clause test", () => {

        it("Simple text test", () => {
            const fragment = sql.fragment`WHERE name = name`
            const params = createParams()

            sql.fragment`SELECT * FROM "users" ${fragment}`.mapIntoQuery(params)

            assert(params.text, `SELECT * FROM "users" WHERE name = name`)
            assert(params.args, [])
        })

        it("Nesting fragment test", () => {
            const roleCondition = sql.fragment`role_name = ${"admin"}`
            const roleSelect = sql.fragment`SELECT id FROM roles where ${roleCondition} limit ${1}`
            
            const params = createParams()

            sql.fragment`INSERT INTO users (role_id) VALUES ((${roleSelect})) WHERE id = ${1}`
                .mapIntoQuery(params)

            assert(
                params.text, 
                "INSERT INTO users (role_id) VALUES ((SELECT id FROM roles where role_name = $1 limit $2)) WHERE id = $3"
            )
            assert(params.args, ["admin", 1, 1])
        })
    })

    describe("Literal clause test", () => {

        it('Literal test', () => {
            const params = createParams()
            
            sql.literal("literal").mapIntoQuery(params)

            assert(params.text, "literal")
            assert(params.args.length, 0)
        })
    })

    describe("Update clause test", () => {

        it("Update clause test", () => {
            const updateMap = {id: 123, name: "Alice"}
            const params = createParams()

            sql.update(updateMap).mapIntoQuery(params)

            assert(params.text, `id = $1, name = $2`)
            assert(params.args, [123, "Alice"])
        })

        it("Undefined behavior test (SKIP)", () => {
            const updateMap = {id: 123, name: undefined}
            const params = createParams()

            sql.update(updateMap).mapIntoQuery(params)

            assert(params.text, `id = $1`)
            assert(params.args, [123])
        })
    })
})
