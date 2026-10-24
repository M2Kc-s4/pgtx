import { env } from 'bun'
import { Pool } from './src'
import { Ok } from 'fluent-future'

const pool = new Pool({
  host: env.PGHOST!,
  port: Number(env.PGPORT),
  user: env.PGUSER!,
  password: env.PGPASSWORD!,
  database: env.PGDATABASE!,
  max: Number(env.PGMAX),
})

interface User {
  id: number
  name: string
  balance: number
}

const server = Bun.serve({
  port: 3000,

  async fetch(req) {
    const { pathname } = new URL(req.url)

    if (pathname === '/users') {
      let counter = 0
      const base = Math.floor(Math.random() * 1000) + 1
      const targetIds = Array.from({ length: 5 }, (_, i) => (base < 950 ? base : 950) + i).sort((a, b) => a - b)

      const users = await pool.query<User>`
        SELECT id, name, balance
        FROM bench_users
        WHERE id = ANY(${targetIds})
        order by id
      `.orElse(err => {
        console.log(err)

        return Ok(null)
      })

      if (!users) {
        return Response.json({ error: 'Error' }, { status: 500 })
      }

      for (const user of users) {
        if (user.id !== targetIds[counter++]) {
          return Response.json({ error: 'Id mismatch' }, { status: 500 })
        }
      }

      return Response.json(users)
    }

    if (pathname === '/user') {
      const targetId = Math.floor(Math.random() * 1000) + 1

      const user = await pool.query<User>`
        SELECT id, name, balance FROM bench_users WHERE id = ${targetId}
      `
      .orElse(err => {
        console.log(err)
        return Ok(null)
      })

      if (!user) {
        return Response.json({ error: 'Timeout' }, { status: 504 })
      }

      if (user[0].id !== targetId) {
        return Response.json({ error: 'ID mismatch' }, { status: 500 })
      }

      return Response.json(user[0])
    }

    if (pathname === '/simple') {
      const [row] = await pool.query<{ res: 1 }>`SELECT 1 as res`
      .orElse(err => {
        console.log(err)
        return Ok([])
      })

      if (!row) {
        return Response.json({ error: 'Timeout' }, { status: 504 })
      }

      if (row.res !== 1) {
        return Response.json({ error: 'Res mismatch' }, { status: 500 })
      }

      return Response.json(row)
    }

    return new Response(null, { status: 404 })
  },
})

console.log(`Pgtx Server running on http://localhost:${server.port}`)