import { after, before, describe, it } from 'node:test';
import assert from 'node:assert';
import { Pool } from '../src'; 


interface UserRow {
    id: number
    name: string
}


describe('Streaming test', () => {
    const pool = new Pool({
        host: process.env.PGHOST!,
        port: Number(process.env.PGPORT),
        user: process.env.PGUSER!,
        password: process.env.PGPASSWORD,
        database: process.env.PGDATABASE!,
        queryTimeout: 500,
        logLevel: 'query'
    })

    before(async () => {
        await pool.query`DROP TABLE IF EXISTS test_stream_users;`
        await pool.query`CREATE TABLE test_stream_users (id SERIAL PRIMARY KEY, name TEXT);`
        await pool.query`
            INSERT INTO test_stream_users (name) 
            VALUES ('Alice'), ('Bob'), ('Charlie'), ('David');
        `
    })

    after(async () => {
        await pool.close()
    })


    it('StreamQuery - Streaming test', async () => {
        const userStream = pool.stream<UserRow>`
            SELECT id, name FROM test_stream_users ORDER BY id ASC
        `

        assert.ok(userStream instanceof ReadableStream)

        const receivedUsers: UserRow[] = []

        for await (const row of userStream) {
            receivedUsers.push(row)
        }

        assert.strictEqual(receivedUsers.length, 4)
        assert.strictEqual(receivedUsers[0].name, 'Alice')
        assert.strictEqual(receivedUsers[1].name, 'Bob')
        assert.strictEqual(receivedUsers[2].name, 'Charlie')
        assert.strictEqual(receivedUsers[3].name, 'David')
    })


    it('StreamQuery - Correct error handling', async () => {
        const invalidStream = pool.stream`SELECT * FROM non_existent_table_abc;`

        try {
            for await (const _ of invalidStream) {
            }
            assert.fail()
        } catch (error: any) {
            assert.strictEqual(error.severity, 'ERROR')
            assert.strictEqual(error.code, '42P01')
        }
    })


    it('StreamQuery - Timeout test', async () => {

        const timeoutStream = pool.stream`SELECT pg_sleep(2);`

        const startTime = Date.now()

        try {
            for await (const _ of timeoutStream) {
            }
            assert.fail()
        } catch (error: any) {
            const duration = Date.now() - startTime
            
            assert.ok(duration >= 450 && duration < 1000)
            assert.strictEqual(error.message, 'Query timeout')
            assert.strictEqual(error.code, '57014')
        }
    })
})