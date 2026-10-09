import { after, describe, it } from "node:test";
import { Connection } from "../src";
import assert from "assert";

describe('Query order test', async () => {
    const conn = await Connection.connect({
        host: process.env.PGHOST!,
        user: process.env.PGUSER!,
        password: process.env.PGPASSWORD!,
        database: process.env.PGDATABASE!,
        port: Number(process.env.PGPORT),
        logLevel: 'none'
    })

    after(async () => {
        await conn.close()
    })


    it("should not skip pipeline after ReadyForQuery", async () => {
        const results = await Promise.all(
            Array.from({length: 100}, async (_, i) => {
                
                return await conn.query`
                    SELECT ${i}::int as value
                `
            }
            )
        )

    
        for (let i = 0; i < 10; i++) {
            assert.strictEqual(
                results[i][0].value,
                i
            )
        }
    })
    

    it("should handle multiple flush batches correctly", async () => {
        const batch1 = []

        for (let i = 0; i < 50; i++) {
            batch1.push(
                conn.query`
                    SELECT ${i}::int as value
                `
            )
        }

        await new Promise(r => setImmediate(r))


        const batch2 = []

        for (let i = 50; i < 100; i++) {
            batch2.push(
                conn.query`
                    SELECT ${i}::int as value
                `
            )
        }


        const results = await Promise.all([
            ...batch1,
            ...batch2
        ])


        for (let i = 0; i < 100; i++) {
            assert.strictEqual(results[i][0].value, i)
        }
    })

    it("should reject pending queries after pipeline error", async () => {
        const queries = [
            conn.query`
                SELECT 1 as value
            `,

            conn.query`
                SELECT * FROM table_that_does_not_exist
            `,

            conn.query`
                SELECT 3 as value
            `
        ]


        const timeout = new Promise((_, reject) =>
            setTimeout(
                () => reject(new Error("Pipeline hung after error")),
                1000
            )
        )


        try {
            await Promise.race([
                Promise.all(queries),
                timeout
            ])

            assert.fail("Expected pipeline to reject")

        } catch (err: any) {
            assert.notStrictEqual(
                err.message,
                "Pipeline hung after error"
            )
        }
    })
})