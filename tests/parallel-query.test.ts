import { describe, it, expectFailure, before, after } from "node:test";
import assert from "node:assert";
import { Connection, Pool } from "../src"; 

describe("Connection Pipeline Race Condition", async () => {
    const pool = new Pool({
        host: process.env.PGHOST!,
        user: process.env.PGUSER!,
        password: process.env.PGPASSWORD!,
        database: process.env.PGDATABASE!,
        port: Number(process.env.PGPORT),
        max: Number(process.env.PGMAX),
    })
    

    before(async () => {
        await pool.begin(async (t) => {
            await t.query`CREATE TABLE IF NOT EXISTS test_courses (id UUID PRIMARY KEY, title TEXT);`
            await t.query`CREATE TABLE IF NOT EXISTS test_topics (id UUID PRIMARY KEY, name TEXT);`
            
            await t.query`TRUNCATE test_courses, test_topics;`;
            await t.query`INSERT INTO test_courses (id, title) VALUES ('00000000-0000-0000-0000-000000000001', 'some course title');`
            await t.query`INSERT INTO test_topics (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'some topic title');`
        })
    })

    after(async () => {
        await pool.close()
    })


    it("should successfully execute two different parameterized queries in parallel (Pipeline Mode)", async () => {
        const courseId = "00000000-0000-0000-0000-000000000001"
        const topicId = "00000000-0000-0000-0000-000000000002"

        const pipeline = Promise.all([
            pool.query`SELECT title FROM test_courses WHERE id = ${courseId}`,
            pool.query`SELECT name FROM test_topics WHERE id = ${topicId}`
        ])

        const timeout = new Promise((_, reject) => 
            setTimeout(() => reject(new Error("TIMEOUT: Pipeline hung in endless PENDING")), 1000)
        )

        try {
            const [courseResult, topicResult] = await Promise.race([pipeline, timeout]) as any


            assert.strictEqual(Array.isArray(courseResult), true)
            assert.strictEqual(courseResult[0].title, 'some course title')

            assert.strictEqual(Array.isArray(topicResult), true)
            assert.strictEqual(topicResult[0].name, 'some topic title')
        } catch (error: any) {
            assert.fail(error.message)
        }
    })

    it("should successfully execute two IDENTICAL parameterized queries in parallel", async () => {
        const courseId = "00000000-0000-0000-0000-000000000001"

        const pipeline = Promise.all([
            pool.query`SELECT title FROM test_courses WHERE id = ${courseId}`,
            pool.query`SELECT title FROM test_courses WHERE id = ${courseId}`
        ])

        const timeout = new Promise((_, reject) => 
            setTimeout(() => reject(new Error("TIMEOUT: Identical queries pipeline hung!")), 1000)
        )

        try {
            const [res1, res2] = await Promise.race([pipeline, timeout]) as any

            assert.strictEqual(res1[0].title, 'some course title')
            assert.strictEqual(res2[0].title, 'some course title')
        } catch (error: any) {
            assert.fail(error.message)
        }
    })
})
