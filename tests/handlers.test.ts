import { describe, it } from "node:test"
import { ErrSocketFailed, Pool } from "../src"
import assert, { rejects } from "assert"

const config = {
    host: process.env.PGHOST!,
    user: process.env.PGUSER!,
    password: process.env.PGPASSWORD!,
    database: process.env.PGDATABASE!,
    port: Number(process.env.PGPORT),
    logLevel: 'none'
} as const


describe("Handlers test", async () => {
    it("should attach defaultHandlers.error to every pooled connection", async () => {
        const errors: unknown[] = []

        const pool = new Pool({
            ...config,
            logLevel: 'none',
            max: 2,
            defaultHandlers: { error: e => { errors.push(e) } },
        })

        const a = await pool.acquire()
        const b = await pool.acquire()

        await rejects(async () => await a.query`SELECT * FROM table_that_does_not_exist_xyz`)
        await rejects(async () => await b.query`SELECT * FROM table_that_does_not_exist_xyz`)
        assert.strictEqual(errors.length, 2)

        pool.release(a)
        pool.release(b)
        await pool.close()
    })

    it("should call defaultHandlers.close once for each connection that drops", async () => {
        let closes = 0
        const pool = new Pool({ ...config, logLevel: 'none', max: 2, defaultHandlers: { close: () => { closes++ } } })

        const a = await pool.acquire()
        const b = await pool.acquire()

        a['_connector'].close()
        b['_connector'].close()
        await new Promise(r => setTimeout(r, 100))

        assert.strictEqual(closes, 2)

        pool.release(a)
        pool.release(b)
        await pool.close()
    })

    it("should report an unexpected drop as ErrSocketFailed, but not a deliberate close", async () => {
        const errors: unknown[] = []
        const pool = new Pool({
            ...config,
            logLevel: 'none',
            max: 2,
            defaultHandlers: { error: e => { errors.push(e) } },
        })

        const a = await pool.acquire()
        const b = await pool.acquire()
        pool.release(b)

        a['_connector'].close()
        await new Promise(r => setTimeout(r, 100))

        assert.strictEqual(errors.length, 1)
        assert.strictEqual(errors[0], ErrSocketFailed)

        pool.release(a)
        await pool.close()   // closes idle b on purpose

        assert.strictEqual(errors.length, 1)
    })
})