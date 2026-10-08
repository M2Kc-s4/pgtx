import { after, describe, it } from "node:test"
import assert, { rejects } from "assert"
import { Pool } from "../src"
import { ErrPoolClosed } from "../src/error"

const config = {
    host: process.env.PGHOST!,
    user: process.env.PGUSER!,
    password: process.env.PGPASSWORD!,
    database: process.env.PGDATABASE!,
    port: Number(process.env.PGPORT),
    logLevel: 'error',
} as const

describe("Pool", () => {

    describe("Dead connections", async () => {
        const pool = new Pool({ ...config, max: 2 })

        after(async () => {
            await pool.close()
        })

        it("should drop a connection with a dead socket on release", async () => {
            const conn = await pool.acquire()
            assert.strictEqual(pool.total, 1)

            conn['_connector']!.close()
            await new Promise(r => setTimeout(r, 100))

            pool.release(conn)

            assert.strictEqual(pool.total, 0)
            assert.strictEqual(pool.size, 0)
        })

        it("should not hand out a dead idle connection from acquire()", async () => {
            const first = await pool.acquire()
            pool.release(first)
            assert.strictEqual(pool.size, 1)

            first['_connector']!.close()
            await new Promise(r => setTimeout(r, 100))

            const second = await pool.acquire()

            assert.notStrictEqual(second, first)
            assert.ok(second.isConnected)

            const result = await second.query`SELECT 1 as value`
            assert.strictEqual(result[0].value, 1)

            pool.release(second)
            assert.strictEqual(pool.total, 1)
        })

        it("should skip dead idle connections in pool.query()", async () => {
            const conn = await pool.acquire()
            pool.release(conn)

            conn['_connector']!.close()
            await new Promise(r => setTimeout(r, 100))

            const result = await pool.query`SELECT 2 as value`
            assert.strictEqual(result[0].value, 2)
        })

        it("should skip dead idle connections in pool.execute()", async () => {
            const conn = await pool.acquire()
            pool.release(conn)

            conn['_connector']!.close()
            await new Promise(r => setTimeout(r, 100))

            await pool.execute`SELECT 1`
        })

        it("should run pool.begin() on a fresh connection after a drop", async () => {
            const conn = await pool.acquire()
            pool.release(conn)

            conn['_connector']!.close()
            await new Promise(r => setTimeout(r, 100))

            const result = await pool.begin(async tx => {
                const rows = await tx.query`SELECT 3 as value`
                return rows[0].value
            })

            assert.strictEqual(result, 3)
        })

        it("should reject in-flight queries when the socket drops and still recover", async () => {
            const conn = await pool.acquire()

            const pending = conn.query`SELECT pg_sleep(0.5), 1 as value`
            const assertion = rejects(async () => await pending)

            conn['_connector']!.close()
            await assertion

            pool.release(conn)

            const result = await pool.query`SELECT 4 as value`
            assert.strictEqual(result[0].value, 4)
        })
    })


    describe("Limits and waiting", async () => {
        it("should never exceed max connections under load", async () => {
            const pool = new Pool({ ...config, max: 2 })

            let peak = 0

            await Promise.all(Array.from({ length: 10 }, (_, i) =>
                pool.begin(async tx => {
                    peak = Math.max(peak, pool.total)
                    const rows = await tx.query`SELECT pg_sleep(0.05), ${i}::int as value`
                    return rows[0].value
                })
            ))

            assert.ok(peak <= 2, `peak was ${peak}`)

            await pool.close()
        })

        it("should serve waiters in order once connections are released", async () => {
            const pool = new Pool({ ...config, max: 1 })

            const conn = await pool.acquire()

            const order: number[] = []
            const w1 = pool.acquire().then(c => { order.push(1); pool.release(c) })
            const w2 = pool.acquire().then(c => { order.push(2); pool.release(c) })

            pool.release(conn)
            await Promise.all([w1, w2])

            assert.deepStrictEqual(order, [1, 2])

            await pool.close()
        })

        it("should give a waiter a fresh connection when the released one is dead", async () => {
            const pool = new Pool({ ...config, max: 1 })

            const conn = await pool.acquire()
            const waiter = pool.acquire()

            conn['_connector']!.close()
            await new Promise(r => setTimeout(r, 100))

            pool.release(conn)

            const fresh = await waiter
            assert.notStrictEqual(fresh, conn)

            const result = await fresh.query`SELECT 5 as value`
            assert.strictEqual(result[0].value, 5)

            pool.release(fresh)
            await pool.close()
        })

        it("should free the slot when a new connection fails to connect", async () => {
            const pool = new Pool({ ...config, port: 1, max: 1 })

            await rejects(async () => await pool.acquire())
            assert.strictEqual(pool.total, 0)

            // slot is free again, so a second attempt must not hang
            await rejects(async () => await pool.acquire())
            assert.strictEqual(pool.total, 0)

            await pool.close()
        })
    })


    describe("Handlers from config", async () => {
        it("should apply onConnect and onError to every pooled connection", async () => {
            let connects = 0
            const errors: unknown[] = []

            const pool = new Pool({
                ...config,
                max: 2,
                onConnect: () => { connects++ },
                onError: e => { errors.push(e) },
            })

            const a = await pool.acquire()
            const b = await pool.acquire()
            assert.strictEqual(connects, 2)

            await rejects(async () => await a.query`SELECT * FROM table_that_does_not_exist_xyz`)
            await rejects(async () => await b.query`SELECT * FROM table_that_does_not_exist_xyz`)
            assert.strictEqual(errors.length, 2)

            pool.release(a)
            pool.release(b)
            await pool.close()
        })

        it("should call onClose for each connection that drops", async () => {
            let closes = 0
            const pool = new Pool({ ...config, max: 2, onClose: () => { closes++ } })

            const a = await pool.acquire()
            const b = await pool.acquire()

            a['_connector']!.close()
            b['_connector']!.close()
            await new Promise(r => setTimeout(r, 100))

            assert.strictEqual(closes, 2)

            pool.release(a)
            pool.release(b)
            await pool.close()
        })
    })


    describe("Close behavior", async () => {
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


    describe("Streaming", async () => {
        const pool = new Pool({ ...config, max: 2 })

        after(async () => {
            await pool.close()
        })

        it("should stream rows through an idle connection", async () => {
            const conn = await pool.acquire()
            pool.release(conn)

            const rows: number[] = []
            for await (const row of pool.stream<{ n: number }>`SELECT generate_series(1, 5) as n`) {
                rows.push(row.n)
            }

            assert.deepStrictEqual(rows, [1, 2, 3, 4, 5])
        })

        it("should stream when there are no idle connections yet", async () => {
            const rows: number[] = []
            for await (const row of pool.stream<{ n: number }>`SELECT generate_series(1, 3) as n`) {
                rows.push(row.n)
            }

            assert.deepStrictEqual(rows, [1, 2, 3])
        })

        it("should stream on a fresh connection after an idle one dropped", async () => {
            const conn = await pool.acquire()
            pool.release(conn)

            conn['_connector']!.close()
            await new Promise(r => setTimeout(r, 100))

            const rows: number[] = []
            for await (const row of pool.stream<{ n: number }>`SELECT generate_series(1, 2) as n`) {
                rows.push(row.n)
            }

            assert.deepStrictEqual(rows, [1, 2])
        })
    })
})