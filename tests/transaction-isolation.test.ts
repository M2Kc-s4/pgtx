import { after, before, describe, it } from "node:test"
import { Pool, sql } from "../src"
import assert, { rejects } from "assert"

const tablename = "transaction_isolation_test"

type Table = { id: number, status: string }

describe("Transaction isolation test", async () => {
    const pool = new Pool({
        host: process.env.PGHOST!,
        user: process.env.PGUSER!,
        password: process.env.PGPASSWORD!,
        database: process.env.PGDATABASE!,
        port: Number(process.env.PGPORT),
        max: Number(process.env.PGMAX),
    })

    
    before(async () => {
        await pool.query`
            create table if not exists ${sql.literal(tablename)} (
                id bigserial primary key,
                status text not null
            );`
        await pool.query`
            truncate table ${sql.literal(tablename)}
        `
    })


    after(async () => {
        await pool.close()
    })

    
    it("transaction test", async () => {
        await pool.begin(async db => {
            await db.query`
                insert into ${sql.ident(tablename)} 
                ${sql.insert<Table>({ id: 1, status: 'success' }, { id: 2, status: "stable" })}`
        })

        const rows = await pool.query<Table>`SELECT * from ${sql.ident(tablename)}`
        assert.deepStrictEqual(rows, [{ id: 1, status: 'success' }, { id: 2, status: "stable" }])
    })


    it("isolation test", async () => {
        await pool.query`truncate table ${sql.ident(tablename)}`

        await rejects(
            async () => {
                await pool.begin(async db => {
                    await db.query`insert into ${sql.ident(tablename)} ${sql.insert<Table>({ id: 1, status: 'success' }, { id: 2, status: "stable" })}`
                    throw new Error("force rollback")
                })
            },
            new Error("force rollback")
        )

        const rows = await pool.query<Table>`SELECT * from ${sql.ident(tablename)}`
        assert.deepStrictEqual(rows, [])
    })


    it("parrallel transaction isolation test", async () => {
        await pool.query`truncate table ${sql.ident(tablename)}`

        await pool.begin(async tx1 => {            
            await pool.begin(async tx2 => {
                await tx2.query`
                    insert into ${sql.ident(tablename)} 
                    ${sql.insert<Table>({ id: 1, status: 'success' }, { id: 2, status: "stable" })}
                `

                const [rowInDb1BeforeCommit] = await tx1.query`SELECT count(*)::int from ${sql.ident(tablename)}`
                assert.deepStrictEqual(rowInDb1BeforeCommit.count, 0)
            }) 

            const [rowInDb1AfterCommit] = await tx1.query`SELECT count(*)::int from ${sql.ident(tablename)}`
            assert.deepStrictEqual(rowInDb1AfterCommit.count, 2)
        })
    })


    it("savepoints isolation test", async () => {
        await pool.query`truncate table ${sql.ident(tablename)}`

        await pool.begin(async db => {
            await db.query`insert into ${sql.ident(tablename)} ${sql.insert<Table>({ id: 1, status: 'success' })}`

            await db.begin(async dbInternal => {
                await dbInternal.query`insert into ${sql.ident(tablename)} ${sql.insert<Table>({ id: 2, status: "stable" })}`

                await rejects(
                    async () => 
                        await dbInternal.begin(async dbDeepInternal => {
                            await dbDeepInternal.query`
                                insert into ${sql.ident(tablename)} 
                                ${sql.insert<Table>({ id: 3, status: 'fail' }, { id: 4, status: "broken" })}
                            `
                            throw new Error("deep level 2 savepoint failed")
                        }),
                    new Error("deep level 2 savepoint failed")
                )
            })

            await rejects(
                async () => 
                    await db.begin(async dbInternal2 => {
                        await dbInternal2.query`insert into ${sql.ident(tablename)} ${sql.insert<Table>({ id: 5, status: 'should_not_exist' })}`
                        throw new Error("level 1 savepoint failed")
                    }),
                new Error("level 1 savepoint failed")
            )
        })

        const rows = await pool.query<Table>`SELECT * from ${sql.ident(tablename)} order by id`
        assert.deepStrictEqual(rows, [
            { id: 1, status: 'success' }, 
            { id: 2, status: "stable" }
        ])
    })
})
