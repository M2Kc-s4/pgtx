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

describe("Listener", () => {
    let notifier: Connection
    const listeners: Listener[] = []

    const make = (extra: Partial<ConstructorParameters<typeof Listener>[0]> = {}) => {
        const listener = new Listener({ ...config, ...extra })
        listeners.push(listener)
        return listener
    }

    before(async () => {
        notifier = new Connection(config)
        await notifier.connect()
    })

    afterEach(async () => {
        await Promise.all(listeners.splice(0).map(l => l.close()))
    })

    after(async () => {
        await notifier.close()
    })


    describe("Subscriptions", () => {
        it("should connect lazily on the first listen() and deliver notifications", async () => {
            const listener = make()
            assert.ok(!listener.isConnected)

            const received: string[] = []
            await listener.listen("ls_basic", p => { received.push(p) })

            assert.ok(listener.isConnected)

            await notifier.query`SELECT pg_notify(${"ls_basic"}, ${"hello"})`
            await sleep(100)

            assert.deepStrictEqual(received, ["hello"])
        })

        it("should add channels on an already connected listener", async () => {
            const listener = make()
            const a: string[] = []
            const b: string[] = []

            await listener.listen("ls_a", p => { a.push(p) })
            await listener.listen("ls_b", p => { b.push(p) })

            await notifier.query`SELECT pg_notify(${"ls_a"}, ${"1"})`
            await notifier.query`SELECT pg_notify(${"ls_b"}, ${"2"})`
            await sleep(100)

            assert.deepStrictEqual([a, b], [["1"], ["2"]])
        })

        it("should handle concurrent listen() calls before the connection is ready", async () => {
            const listener = make()
            const a: string[] = []
            const b: string[] = []

            await Promise.all([
                listener.listen("ls_c1", p => { a.push(p) }),
                listener.listen("ls_c2", p => { b.push(p) }),
            ])

            await notifier.query`SELECT pg_notify(${"ls_c1"}, ${"x"})`
            await notifier.query`SELECT pg_notify(${"ls_c2"}, ${"y"})`
            await sleep(100)

            assert.deepStrictEqual([a, b], [["x"], ["y"]])
        })

        it("should replace the handler when listening to the same channel again", async () => {
            const listener = make()
            const first: string[] = []
            const second: string[] = []

            await listener.listen("ls_replace", p => { first.push(p) })
            await listener.listen("ls_replace", p => { second.push(p) })

            await notifier.query`SELECT pg_notify(${"ls_replace"}, ${"v"})`
            await sleep(100)

            assert.strictEqual(first.length, 0)
            assert.deepStrictEqual(second, ["v"])
        })

        it("should stop delivering after unlisten()", async () => {
            const listener = make()
            const received: string[] = []

            await listener.listen("ls_unlisten", p => { received.push(p) })

            await notifier.query`SELECT pg_notify(${"ls_unlisten"}, ${"before"})`
            await sleep(100)
            assert.deepStrictEqual(received, ["before"])

            await listener.unlisten("ls_unlisten")

            await notifier.query`SELECT pg_notify(${"ls_unlisten"}, ${"after"})`
            await sleep(100)
            assert.deepStrictEqual(received, ["before"])
        })

        it("should not fail on unlisten() of an unknown channel", async () => {
            const listener = make()
            await listener.unlisten("ls_never_listened")
        })
    })


    describe("Reconnect", () => {
        it("should reconnect and restore every LISTEN after the socket drops", async () => {
            const listener = make()
            const a: string[] = []
            const b: string[] = []

            await listener.listen("ls_re_a", p => { a.push(p) })
            await listener.listen("ls_re_b", p => { b.push(p) })

            listener['_conn']['_connector']!.close()
            await sleep(100)
            assert.ok(!listener.isConnected)

            // first backoff step is 1s
            await sleep(1500)
            assert.ok(listener.isConnected)

            await notifier.query`SELECT pg_notify(${"ls_re_a"}, ${"1"})`
            await notifier.query`SELECT pg_notify(${"ls_re_b"}, ${"2"})`
            await sleep(100)

            assert.deepStrictEqual([a, b], [["1"], ["2"]])
        })

        it("should reconnect immediately on an explicit listen() during backoff", async () => {
            const listener = make()
            await listener.listen("ls_back_a", () => {})

            listener['_conn']['_connector']!.close()
            await sleep(100)

            const received: string[] = []
            const start = Date.now()
            await listener.listen("ls_back_b", p => { received.push(p) })

            assert.ok(Date.now() - start < 900, "should not wait for the backoff timer")
            assert.ok(listener.isConnected)

            await notifier.query`SELECT pg_notify(${"ls_back_b"}, ${"now"})`
            await sleep(100)
            assert.deepStrictEqual(received, ["now"])
        })

        it("should keep a handler registered when the first connect fails, and drop it from the caller's error", async () => {
            const listener = make({ port: 1 })

            await rejects(async () => await listener.listen("ls_fail", () => {}))

            assert.ok(!listener.isConnected)
            assert.strictEqual(listener['_handlers']["ls_fail"], undefined)
        })

        it("should not reconnect after close(), even during backoff", async () => {
            const listener = make()
            await listener.listen("ls_close", () => {})

            listener['_conn']['_connector']!.close()
            await sleep(100)

            await listener.close()

            await sleep(1300)
            assert.ok(!listener.isConnected)
        })
    })


    describe("Close and config", () => {
        it("should close the underlying connection on close()", async () => {
            const listener = make()
            await listener.listen("ls_closing", () => {})

            await listener.close()

            assert.ok(listener['_conn'].isClosed)
        })

        it("should reject listen() after close()", async () => {
            const listener = make()
            await listener.close()

            await rejects(async () => await listener.listen("ls_after_close", () => {}), ErrConnectionClosed)
        })

        it("should forward the user's onConnect and onClose from the config", async () => {
            let connects = 0
            let closes = 0

            const listener = make({
                onConnect: () => { connects++ },
                onClose: () => { closes++ },
            })

            await listener.listen("ls_forward", () => {})
            assert.strictEqual(connects, 1)

            listener['_conn']['_connector']!.close()
            await sleep(100)
            assert.strictEqual(closes, 1)

            // reconnect fires onConnect again
            await sleep(1500)
            assert.strictEqual(connects, 2)
        })
    })
})