import { sql, Pool } from '@m2k-5f/pgtx';
import { env } from 'node:process';
import postgres from 'postgres';

const DB = {
  host: env.PGHOST!,
  port: Number(env.PGPORT!),
  user: env.PGUSER!,
  password: env.PGPASSWORD!,
  database: env.PGDATABASE!,
}

const pgtx = new Pool({ ...DB, max: 20 })

const pg = postgres({ ...DB, max: 20 })

const bunSql = new Bun.sql({...DB, max: 20})


const server = Bun.serve({
  port: 3000,

  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname.slice(1)

    const json = (data: any, status = 200) => {
      return new Response(JSON.stringify(data), {
        status,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    try {
      let rows: any[] = []

      switch (path) {
        case 'pgtx': {
          const result = await pgtx.query`
            SELECT id, name, balance 
            FROM users 
            ORDER BY id 
            LIMIT 30
          `
          rows = result
        } break

        case 'postgresjs': {
          const result = await pg`
            SELECT id, name, balance 
            FROM users 
            ORDER BY id 
            LIMIT 30
          `
          rows = result
        } break

        case 'bunsql': {
          const result = await bunSql`
            SELECT id, name, balance 
            FROM users 
            ORDER BY id 
            LIMIT 30
          `
          rows = result
        } break

        default:
          return json({
            drivers: ['pgtx', 'postgresjs', 'bunsql'],
            endpoints: {
              pgtx: '/pgtx',
              postgresjs: '/postgresjs',
              bunsql: '/bunsql',
            },
          })
      }

      return json({
        driver: path,
        count: rows.length,
        rows,
      })
    } catch (err: any) {
      return json({ error: err.message }, 500)
    }
  },
})

console.log(`Bun server: http://localhost:${server.port}`)

process.on('SIGINT', () => {
  pgtx.close()
  pg.end()
  bunSql.close()
  server.stop()
  process.exit(0)
})