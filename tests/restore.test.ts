import { after, describe, it } from "node:test"
import { ErrConnectionClosed, PostgresError } from "../src/error"
import assert, { rejects } from "assert"
import { Future, Ok } from "fluent-future"
import { Pool, sql } from "../src"

describe("Connection reconnect and close test", async () => {
    const pool = new Pool({
        host: process.env.PGHOST!,
        user: process.env.PGUSER!,
        password: process.env.PGPASSWORD!,
        database: process.env.PGDATABASE!,
        port: Number(process.env.PGPORT),
        max: 2
    })

    after(async () => {
        await pool.close()
    })

    describe("Reconnect behavior", async () => {
        const conn = await pool.acquire()

        after(async () => {
            pool.release(conn)
        })


        it("should recover and serve queries after the socket is forcibly destroyed", async () => {
            const before = await conn.query`SELECT 1 as value`
            assert.strictEqual(before[0].value, 1)

            conn['_connector'].destroy()

            await new Promise(r => setTimeout(r, 500))

            const after = await conn.query`SELECT 2 as value`
            assert.strictEqual(after[0].value, 2)
        })

        it("should queue and resolve queries issued during an active reconnect", async () => {
            conn['_connector'].destroy()

            await new Promise(r => setTimeout(r, 500))

            const results = await Promise.all(
                Array.from({length: 10}, (_, i) => conn.query`SELECT ${i}::int as value`)
            )

            for (let i = 0; i < 10; i++) {
                assert.strictEqual(results[i][0].value, i)
            }
        })

        it("should clear cached prepared statement metadata on reconnect", async () => {
            await conn.query`SELECT 1 as value`

            const parsedBefore = conn['_parsed'].size
            assert.ok(parsedBefore > 0)

            conn['_connector'].destroy()

            await new Promise(r => setTimeout(r, 50))

            const result = await conn.query`SELECT 1 as value`
            
            assert.ok(conn['_parsed'].size === 1)
            assert.strictEqual(result[0].value, 1)
        })

        it("should restore LISTEN subscriptions after reconnect", async () => {
            const received: string[] = []

            await conn.listen("reconnect_channel", payload => {
                received.push(payload)
            })

            conn['_connector'].destroy()

            await new Promise(r => setTimeout(r, 500))

            await conn.notify("reconnect_channel", "hello-after-reconnect")

            await new Promise(r => setTimeout(r, 100))

            assert.deepStrictEqual(received, ["hello-after-reconnect"])
        })

        it("should reject in-flight batch queue entries with ErrConnectionReconnecting on drop", async () => {
            const pending = conn.query`SELECT pg_sleep(0.5), 1 as value`

            conn['_connector'].destroy()

            await rejects(async () => await pending)
            await new Promise(r => setTimeout(r, 500))
        })

        it("should keep retrying reconnect if the first attempt fails", async () => {
            let attempts = 0
            const originalPerformReconnect = conn['_performReconnect'].bind(conn)

            conn['_performReconnect'] = () => {
                attempts++

                if (attempts === 1) {
                    conn['_parsed'].clear()
                    conn['_parsing'].clear()
                    return Future.reject(new PostgresError("simulated reconnect failure"))
                }

                conn['_performReconnect'] = originalPerformReconnect
                return originalPerformReconnect()
            }
            

            conn['_connector'].destroy()

            await new Promise(r => setTimeout(r, 500))
            
            assert.ok(attempts === 2, `expected at least 2 reconnect attempts, got ${attempts}`)

            const result = await conn.query`SELECT 1 as value`
            
            assert.strictEqual(result[0].value, 1)
        })
    })


    describe("Close behavior", async () => {
        it("should resolve immediately when closing an already-idle connection", async () => {
            const conn = await pool.acquire()

            await conn.query`SELECT 1 as value`

            const start = Date.now()
            await conn.close()
            const elapsed = Date.now() - start

            assert.ok(conn.isClosed)
            assert.ok(!conn.isOpened)
            assert.ok(elapsed < 100)
            
            pool.release(conn)
        })

        it("should reject new queries immediately after close", async () => {
            const conn = await pool.acquire()
            await conn.close()

            await rejects(
                async () => await conn.query`SELECT 1 as value`,
                ErrConnectionClosed
            )

            pool.release(conn)
        })

        it("should be idempotent when close is called multiple times", async () => {
            const conn = await pool.acquire()

            await conn.close()
            await conn.close()

            assert.ok(conn.isClosed)
            
            pool.release(conn)
        })

        it("should wait for in-flight queries to finish before closing", async () => {
            const conn = await pool.acquire()

            const pending = conn.query`SELECT pg_sleep(0.2), 1 as value`

            const closePromise = conn.close()

            assert.ok(conn.isClosed) 
            assert.ok(!conn.isOpened)

            const result = await pending
            assert.strictEqual(result[0].value, 1)

            await closePromise
            assert.ok(conn.isClosed)
            
            pool.release(conn)
        })

        it("should reject queries issued while a drain-close is pending", async () => {
            const conn = await pool.acquire()

            const pending = conn.query`SELECT pg_sleep(0.2), 1 as value`
            const closePromise = conn.close()

            await rejects(
                async () => await conn.query`SELECT 2 as value`,
                ErrConnectionClosed
            )

            await pending
            await closePromise
            
            pool.release(conn)
        })

        it("should destroy the underlying socket only after the batch queue is fully drained", async () => {
            const conn = await pool.acquire()

            const q1 = conn.query`SELECT pg_sleep(0.1), 1 as value`
            const q2 = conn.query`SELECT pg_sleep(0.1), 2 as value`

            const closePromise = conn.close()

            const destroySpy = { called: false }
            const originalDestroy = conn['_connector'].destroy.bind(conn['_connector'])
            conn['_connector'].destroy = () => {
                destroySpy.called = true
                return originalDestroy()
            }

            assert.strictEqual(destroySpy.called, false)

            await Promise.all([q1, q2])
            await closePromise

            assert.strictEqual(destroySpy.called, true)
            
            pool.release(conn)
        })

        it("should not attempt to reconnect after the connection has been closed", async () => {
            const conn = await pool.acquire()
            await conn.close()
            

            let reconnectCalled = false

            conn['_performReconnect'] = () => {reconnectCalled = true; return Ok()}

            conn['_connector'].destroy()

            await new Promise(r => setTimeout(r, 50))
            assert.strictEqual(reconnectCalled, false)
            
            pool.release(conn)
        })

        it("should not close before the follow-up query queued right after Parse resolves (parse/bind race window)", async () => {
            const conn = await pool.acquire()

            const pending = conn.query`SELECT pg_sleep(0.05)::text, 12345 as value -- race-${sql.literal(Date.now().toString())}-${sql.literal(Math.random().toString())}`

            const closePromise = conn.close()

            assert.ok(conn.isClosed)
            assert.ok(!conn.isOpened)

            const result = await pending
            assert.strictEqual(result[0].value, 12345)

            await closePromise
            assert.ok(conn.isClosed)

            pool.release(conn)
        })
    })
})