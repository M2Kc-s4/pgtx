import { after, before, describe, it } from "node:test"
import { Connection,  sql } from "../src"
import assert from "assert"

const table = "pipeline_error_recovery_test"

describe("Error handling test", async () => {
    const conn  = await Connection.connect({
        host: process.env.PGHOST!,
        user: process.env.PGUSER!,
        password: process.env.PGPASSWORD!,
        database: process.env.PGDATABASE!,
        port: Number(process.env.PGPORT),
        int8toBigint: true,
        logLevel: 'none'
    })


    before(async () => {
        await conn.query`
            create table if not exists ${sql.ident(table)} (
                id integer primary key,
                username text unique not null,
                balance integer not null
            );`

        await conn.query`truncate table ${sql.ident(table)}`

        await conn.query`
            insert into ${sql.ident(table)} (id, username, balance)
            values (1, 'alice', 1000)
        `
    })


    after(async () => {
        await conn.close()
    })


    it("Resolves the leading query and rejects the failing one in a batch", async () => {
        const q1 = conn.query`UPDATE ${sql.ident(table)} SET balance = 1100 WHERE id = 1`
        const q2 = conn.query`INSERT INTO ${sql.ident(table)} (id, username, balance) VALUES (1, 'fail', 0)` // already exists -> throws
        const q3 = conn.query<any>`SELECT balance, username FROM ${sql.ident(table)} WHERE id = 1`

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


    it("Resolves the leading query and rejects the failing tail query when the retry itself errors", async () => {
        const q1 = conn.query`UPDATE ${sql.ident(table)} SET balance = 500 WHERE id = 1`
        const q2 = conn.query`INSERT INTO ${sql.ident(table)} (id, username, balance) VALUES (1, 'fail', 0)`
        const q3 = conn.query`SELECT uncorrect_tablename FROM ${sql.ident(table)}`

        let q2Error: any = null
        let q3Error: any = null

        await Promise.all([
            q1,
            q2.catch(err => q2Error = err),
            q3.catch(err => q3Error = err)
        ])

        assert.strictEqual(q2Error?.code, '23505')
        assert.strictEqual(q3Error?.code, '42703')
        
        const verify = await conn.query<any>`SELECT balance FROM ${sql.ident(table)} WHERE id = 1`

        assert.strictEqual(verify[0].balance, 500)
    })
})