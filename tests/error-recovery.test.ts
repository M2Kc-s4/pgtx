import { after, before, describe, it } from "node:test"
import { Connection,  sql } from "../src"
import assert from "assert"

describe("PostgreSQL Pipeline Batching Error Recovery (Monaic Retry Feature)", async () => {
    const conn  = await Connection.connect({
        host: process.env.PGHOST!,
        user: process.env.PGUSER!,
        password: process.env.PGPASSWORD!,
        database: process.env.PGDATABASE!,
        port: Number(process.env.PGPORT),
        int8toBigint: true
    })

    const pipelineTestTable = "pipeline_error_recovery_test"

    before(async () => {
        await conn.query`
            create table if not exists ${sql.ident(pipelineTestTable)} (
                id integer primary key,
                username text unique not null,
                balance integer not null
            );`

        await conn.query`truncate table ${sql.ident(pipelineTestTable)}`

        await conn.query`
            insert into ${sql.ident(pipelineTestTable)} (id, username, balance)
            values (1, 'alice', 1000)
        `
    })

    after(async () => {
        await conn.close()
    })

    it("resolves the leading query and rejects the failing one in a batch", async () => {
        const q1 = conn.query`UPDATE ${sql.ident(pipelineTestTable)} SET balance = 1100 WHERE id = 1`
        const q2 = conn.query`INSERT INTO ${sql.ident(pipelineTestTable)} (id, username, balance) VALUES (1, 'fail', 0)`
        const q3 = conn.query<any>`SELECT balance, username FROM ${sql.ident(pipelineTestTable)} WHERE id = 1`

        let q2Error: any = null

        const [r1, r2, r3] = await Promise.all([
            q1,
            q2.catch(err => { q2Error = err; return null }),
            q3
        ])

        assert.strictEqual(q2Error?.code, '23505')

        assert.strictEqual(r3[0].balance, 1100)
        assert.strictEqual(r3[0].username, 'alice')
    })

    it("keeps the committed update visible to a fresh query on the same connection", async () => {
        const verify = await conn.query<any>`SELECT balance FROM ${sql.ident(pipelineTestTable)} WHERE id = 1`

        assert.strictEqual(verify[0].balance, 1100)
    })

    it("resolves the leading query and rejects the failing tail query when the retry itself errors", async () => {
        const q1 = conn.query`UPDATE ${sql.ident(pipelineTestTable)} SET balance = 500 WHERE id = 1`
        const q2 = conn.query`INSERT INTO ${sql.ident(pipelineTestTable)} (id, username, balance) VALUES (1, 'fail', 0)`
        const q3 = conn.query`SELECT uncorrect_tablename FROM ${sql.ident(pipelineTestTable)}`

        let q2Error: any = null
        let q3Error: any = null

        await Promise.all([
            q1,
            q2.catch(err => q2Error = err),
            q3.catch(err => q3Error = err)
        ])

        assert.strictEqual(q2Error?.code, '23505')
        assert.strictEqual(q3Error?.code, '42703')
    })

    it("keeps the second committed update visible despite two errors in one batch", async () => {
        const verify = await conn.query<any>`SELECT balance FROM ${sql.ident(pipelineTestTable)} WHERE id = 1`

        assert.strictEqual(verify[0].balance, 500)
    })
})