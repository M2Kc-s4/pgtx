import { after, afterEach, before, describe, it } from "node:test"
import assert, { rejects } from "assert"
import { Connection } from "../src"
import { Listener } from "../src/listener"
import { ErrConnectionClosed } from "../src/error"

const config = {
    host: process.env.PGHOST!,
    user: process.env.PGUSER!,
    password: process.env.PGPASSWORD!,
    database: process.env.PGDATABASE!,
    port: Number(process.env.PGPORT),
    logLevel: 'none' as const,
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

describe("Listener test", async () => {
    const notifier = await Connection.connect(config)

    after(async () => {
        await notifier.close()
    })


    describe("Subscriptions", () => {

        it("Should connect lazily on the first listen() and deliver notifications", async () => {
            const listener = new Listener(config)
            assert.strictEqual(listener['_connection'], null)

            const received: string[] = []
            await listener.listen("x", p => { received.push(p) })

            assert.ok(listener['_connection']!.isOpened)

            await notifier.query`SELECT pg_notify(${"x"}, ${"hello"})`
            await sleep(100)

            assert.deepStrictEqual(received, ["hello"])

            await listener.close()
        })

        it("should add channels on an already connected listener", async () => {
            const listener = new Listener(config)
            const a: string[] = []
            const b: string[] = []

            await listener.listen("a", p => a.push(p))
            await listener.listen("b", p => b.push(p))

            await notifier.query`SELECT pg_notify(${"a"}, ${"1"})`
            await notifier.query`SELECT pg_notify(${"b"}, ${"2"})`
            await sleep(100)

            assert.deepStrictEqual([a, b], [["1"], ["2"]])

            await listener.close()
        })

        it("should share one connection between concurrent listen() calls", async () => {
            const listener = new Listener(config)
            const a: string[] = []
            const b: string[] = []

            const first = listener.listen("c1", p => a.push(p))
            const connecting = listener['_connecting']
            const second = listener.listen("c2", p => { b.push(p) })

            assert.ok(connecting)
            assert.strictEqual(listener['_connecting'], connecting)

            await Promise.all([first, second])

            await notifier.query`SELECT pg_notify(${"c1"}, ${"x"})`
            await notifier.query`SELECT pg_notify(${"c2"}, ${"y"})`
            await sleep(100)

            assert.deepStrictEqual([a, b], [["x"], ["y"]])

            await listener.close()
        })

        it("should call every handler registered on the same channel", async () => {
            const listener = new Listener(config)
            const first: string[] = []
            const second: string[] = []

            await listener.listen("ls_multi", p => { first.push(p) })
            await listener.listen("ls_multi", p => { second.push(p) })

            await notifier.query`SELECT pg_notify(${"ls_multi"}, ${"v"})`
            await sleep(100)

            assert.deepStrictEqual(first, ["v"])
            assert.deepStrictEqual(second, ["v"])

            await listener.close()
        })

        it("should not call the same handler twice when it is registered twice", async () => {
            const listener = new Listener(config)
            const received: string[] = []
            const handler = (p: string) => { received.push(p) }

            await listener.listen("ls_dup", handler)
            await listener.listen("ls_dup", handler)

            await notifier.query`SELECT pg_notify(${"ls_dup"}, ${"once"})`
            await sleep(100)

            assert.deepStrictEqual(received, ["once"])

            await listener.close()
        })

        it("should remove only the given handler and keep listening for the rest", async () => {
            const listener = new Listener(config)
            const a: string[] = []
            const b: string[] = []
            const handlerA = (p: string) => { a.push(p) }
            const handlerB = (p: string) => { b.push(p) }

            await listener.listen("ls_partial", handlerA)
            await listener.listen("ls_partial", handlerB)

            await listener.unlisten("ls_partial", handlerA)

            await notifier.query`SELECT pg_notify(${"ls_partial"}, ${"v"})`
            await sleep(100)

            assert.deepStrictEqual(a, [])
            assert.deepStrictEqual(b, ["v"])

            // the last handler is gone: the channel is UNLISTENed
            await listener.unlisten("ls_partial", handlerB)
            assert.ok(!listener['_handlers'].has("ls_partial"))

            await notifier.query`SELECT pg_notify(${"ls_partial"}, ${"after"})`
            await sleep(100)

            assert.deepStrictEqual(b, ["v"])
            
            await listener.close()
        })

        it("should stop delivering to every handler after unlisten() without a handler", async () => {
            const listener = new Listener(config)
            const a: string[] = []
            const b: string[] = []

            await listener.listen("ls_unlisten", p => { a.push(p) })
            await listener.listen("ls_unlisten", p => { b.push(p) })

            await notifier.query`SELECT pg_notify(${"ls_unlisten"}, ${"before"})`
            await sleep(100)
            assert.deepStrictEqual([a, b], [["before"], ["before"]])

            await listener.unlisten("ls_unlisten")

            await notifier.query`SELECT pg_notify(${"ls_unlisten"}, ${"after"})`
            await sleep(100)
            assert.deepStrictEqual([a, b], [["before"], ["before"]])
            
            await listener.close()
        })

        it("should not fail on unlisten() of an unknown channel", async () => {
            const listener = new Listener(config)
            await listener.unlisten("ls_never_listened")
            
            await listener.close()
        })
    })


    describe("Reconnect", () => {
        it("should reconnect and restore every LISTEN after the socket drops", async () => {
            const listener = new Listener(config)
            const a: string[] = []
            const b: string[] = []

            await listener.listen("ls_re_a", p => { a.push(p) })
            await listener.listen("ls_re_b", p => { b.push(p) })

            const old = listener['_connection']!
            old['_connector'].close()
            await sleep(100)
            assert.strictEqual(listener['_connection'], null)

            // first backoff step is 1s
            await sleep(1500)
            assert.ok(listener['_connection']!.isOpened)
            assert.notStrictEqual(listener['_connection'], old)

            await notifier.query`SELECT pg_notify(${"ls_re_a"}, ${"1"})`
            await notifier.query`SELECT pg_notify(${"ls_re_b"}, ${"2"})`
            await sleep(100)

            assert.deepStrictEqual([a, b], [["1"], ["2"]])
            
            await listener.close()
        })

        it("should reconnect immediately on an explicit listen() during backoff", async () => {
            const listener = new Listener(config)
            await listener.listen("ls_back_a", () => {})

            listener['_connection']!['_connector'].close()
            await sleep(100)

            const received: string[] = []
            const start = Date.now()
            await listener.listen("ls_back_b", p => { received.push(p) })

            assert.ok(Date.now() - start < 900, "should not wait for the backoff timer")
            assert.ok(listener['_connection']?.isOpened)

            await notifier.query`SELECT pg_notify(${"ls_back_b"}, ${"now"})`
            await sleep(100)
            assert.deepStrictEqual(received, ["now"])
            
            await listener.close()
        })

        it("should reject listen() and drop the handler when the first connect fails", async () => {
            const listener = new Listener({ port: 1 , host: 'localhost', database: 'uncorrect', user: "dsfsffs"})

            await rejects(async () => await listener.listen("ls_fail", () => {}))

            assert.strictEqual(listener['_connection'], null)
            assert.ok(!listener['_handlers'].has("ls_fail"))
            
            await listener.close()
        })

        it("should not reconnect after close(), even during backoff", async () => {
            const listener = new Listener(config)
            await listener.listen("ls_close", () => {})

            listener['_connection']!['_connector'].close()
            await sleep(100)

            await listener.close()

            await sleep(1300)
            assert.strictEqual(listener['_connection'], null)
            assert.strictEqual(listener['_connecting'], null)
            
            await listener.close()
        })
    })


    describe("Close", () => {
        it("should close the underlying connection on close()", async () => {
            const listener = new Listener(config)
            await listener.listen("ls_closing", () => {})

            const conn = listener['_connection']!
            await listener.close()

            assert.ok(conn.isClosed)
            assert.ok(listener.isClosed)
            assert.ok(!listener.isOpened)
            
            await listener.close()
        })

        it("should drop all handlers on close()", async () => {
            const listener = new Listener(config)
            await listener.listen("ls_drop", () => {})

            await listener.close()

            assert.strictEqual(listener['_handlers'].size, 0)
            
            await listener.close()
        })

        it("should reject listen() after close()", async () => {
            const listener = new Listener(config)
            await listener.close()

            await rejects(async () => await listener.listen("ls_after_close", () => {}), ErrConnectionClosed)
            
            await listener.close()
        })

        it("should be safe to call close() twice", async () => {
            const listener = new Listener(config)
            await listener.listen("ls_twice", () => {})

            await listener.close()
            await listener.close()

            assert.ok(listener.isClosed)
            
            await listener.close()
        })
    })
})