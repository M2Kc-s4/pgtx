import { describe, it } from "node:test"
import { ErrPoolClosed, Pool } from "../src"
import assert, { rejects } from "assert"

const config = {
    host: process.env.PGHOST!,
    user: process.env.PGUSER!,
    password: process.env.PGPASSWORD!,
    database: process.env.PGDATABASE!,
    port: Number(process.env.PGPORT),
    logLevel: 'none',
} as const

describe("Close behavior test", async () => {
    it("should close idle connections and reset counters", async () => {
        const pool = new Pool({ ...config, max: 2 })

        const a = await pool.acquire()
        const b = await pool.acquire()
        pool.release(a)
        pool.release(b)
        assert.strictEqual(pool.size, 2)

        await pool.close()

        assert.ok(pool.isClosed)
        assert.ok(!pool.isOpened)
        assert.ok(a.isClosed)
        assert.ok(b.isClosed)
        assert.strictEqual(pool.size, 0)
        assert.strictEqual(pool.total, 0)
    })

    it("should be safe to call close() twice", async () => {
        const pool = new Pool({ ...config, max: 1 })

        await pool.close()
        await pool.close()

        assert.ok(pool.isClosed)
    })

    it("should reject everything after close", async () => {
        const pool = new Pool({ ...config, max: 1 })
        await pool.close()

        await rejects(async () => await pool.acquire(), ErrPoolClosed)
        await rejects(async () => await pool.query`SELECT 1`, ErrPoolClosed)
        await rejects(async () => await pool.execute`SELECT 1`, ErrPoolClosed)
        await rejects(async () => await pool.begin(async () => {}), ErrPoolClosed)
        assert.throws(() => pool.stream`SELECT 1`, ErrPoolClosed)
    })

    it("should reject pending acquire() calls on close", async () => {
        const pool = new Pool({ ...config, max: 1 })

        const conn = await pool.acquire()
        const waiter = pool.acquire()
        const assertion = rejects(async () => await waiter, ErrPoolClosed)

        await pool.close()
        await assertion

        pool.release(conn)
    })

    it("should close a connection that is released after the pool is closed, without throwing", async () => {
        const pool = new Pool({ ...config, max: 1 })

        const conn = await pool.acquire()
        await pool.close()

        assert.doesNotThrow(() => pool.release(conn))

        await new Promise(r => setTimeout(r, 100))
        assert.ok(conn.isClosed)
    })

    it("should keep the original result when release happens after close inside withAcquire", async () => {
        const pool = new Pool({ ...config, max: 1 })

        const result = await pool.withAcquire(async conn => {
            const rows = await conn.query`SELECT 6 as value`
            await pool.close()
            return rows[0].value
        })

        assert.strictEqual(result, 6)
    })
})
