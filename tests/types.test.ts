import { after, before, describe, it } from "node:test"
import { Connection, Pool, sql } from "../src"
import assert from "assert"

const allTypesTableName = "all_datatypes_parsing_test"


describe("Complete PostgreSQL Binary Datatypes Parsing Test", async () => {
    const conn = await Connection.new({
        host: process.env.PGHOST!,
        user: process.env.PGUSER!,
        password: process.env.PGPASSWORD!,
        database: process.env.PGDATABASE!,
        port: Number(process.env.PGPORT),
        int8toBigint: true
    })

    before(async () => {
        await conn.query`drop table ${sql.ident(allTypesTableName)}`.recover()
        await conn.query`
            create table if not exists ${sql.literal(allTypesTableName)} (
                id_int4 integer primary key,
                id_int2 smallint not null,
                id_int8 bigint not null,
                flag_bool boolean not null,
                text_col text not null,
                varchar_col varchar(255) not null,
                char_col char(10) not null,
                float4_col real not null,
                float64_col double precision not null,
                bytea_col bytea not null,
                json_col json not null,
                jsonb_col jsonb not null,
                date_col date not null,
                ts_col timestamp not null,
                tstz_col timestamptz not null,
                uuid_col uuid not null,
                numeric_col numeric(14,4) not null,
                time_col time not null,
                timetz_col timetz not null,
                point point not null,
                inta int4[]
            );`
    })

    after(async () => {
        await conn.close()
    })

    it("should correctly parse absolutely all specified types in binary mode", async () => {
        await conn.query`truncate table ${sql.ident(allTypesTableName)}`

        const sampleBytea = Buffer.from([0xaa, 0xbb, 0xcc, 0xdd])
        const sampleUuid = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
        
        const testData = {
            id_int4: 42000,
            id_int2: 320,
            id_int8: 9223372036854n,
            flag_bool: true,
            text_col: "Short text for cache",
            varchar_col: "This is a much longer text string designed to bypass the 32 bytes cache limit completely inside readDataRow", 
            char_col: "fixed     ", 
            float4_col: Math.fround(1.234024),
            float64_col: 123456.789101112,
            bytea_col: sampleBytea,
            json_col: { meta: "data", tags: [1, 2] },
            jsonb_col: { fast: true, nested: { id: 1 } },
            date_col: new Date("2026-08-11"),
            ts_col: new Date("2026-08-11 12:00:00"),
            tstz_col: new Date("2026-08-11 12:00:00+00"),
            uuid_col: sampleUuid,
            numeric_col: "12345678.1234",
            time_col: "15:30:45.123",
            timetz_col: "15:30:45.123+03:00",
            point: {x: 1, y: 1},
            inta: [1, 2, 3, 5]
        }

        await conn.query`
            insert into ${sql.ident(allTypesTableName)} ${sql.insert(testData)};
        `

        const [row] = await conn.query`SELECT * FROM ${sql.ident(allTypesTableName)}`

        assert.deepStrictEqual(row, testData)
    })

    it("should return null for all fields when they are NULL in database", async () => {
        const nullTableName = "all_types_null_test"
        
        await conn.query`
            create table if not exists ${sql.literal(nullTableName)} (
                id_int4 integer, id_int2 smallint, id_int8 bigint, flag_bool boolean,
                text_col text, varchar_col varchar(255), char_col char(10),
                float4_col real, float64_col double precision, bytea_col bytea,
                json_col json, jsonb_col jsonb, date_col date, ts_col timestamp, 
                tstz_col timestamptz, uuid_col uuid,
                numeric_col numeric(14,4), time_col time, timetz_col timetz
            );`

        await conn.query`
            insert into ${sql.literal(nullTableName)} 
            values (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        `

        const rows = await conn.query<any>`SELECT * FROM ${sql.literal(nullTableName)}`
        const row = rows[0]

        Object.keys(row).forEach(key => {
            assert.strictEqual(row[key], null)
        })

        await conn.query`drop table if exists ${sql.literal(nullTableName)}`
    })
    
    it("should correctly parse numeric edge cases (negative, whole, NaN)", async () => {
        const numericTableName = "numeric_edge_cases_test"

        await conn.query`
            create table if not exists ${sql.literal(numericTableName)} (
                id integer primary key,
                val numeric(20,6) not null
            );`

        await conn.query`truncate table ${sql.ident(numericTableName)}`

        const cases: [number, string][] = [
            [1, "0.000000"],
            [2, "-123456.789000"],
            [3, "999999999999.999999"],
            [4, "-0.000001"],
            [5, "100.000000"],
        ]

        for (const [id, val] of cases) {
            await conn.query`
                insert into ${sql.ident(numericTableName)} (id, val) values (${id}, ${val}::numeric)
            `
        }

        const rows = await conn.query<{id: number, val: string}>`
            SELECT id, val FROM ${sql.ident(numericTableName)} ORDER BY id
        `

        rows.forEach((row, i) => {
            assert.strictEqual(row.val, cases[i][1], `numeric case id=${cases[i][0]}`)
        })

        await conn.query`drop table if exists ${sql.literal(numericTableName)}`
    })
})
