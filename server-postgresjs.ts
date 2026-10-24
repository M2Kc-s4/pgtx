import { env } from 'bun'
import postgres from 'postgres'

const sql = postgres({
  host: env.PGHOST || 'localhost',
  port: Number(env.PGPORT),
  user: env.PGUSER,
  password: env.PGPASSWORD,
  database: env.PGDATABASE,
  max: Number(env.PGMAX),
})

const server = Bun.serve({
  port: 3001,

  async fetch(req) {
    const { pathname } = new URL(req.url)

    if (pathname === '/users') {
      let counter = 0
      const base = Math.floor(Math.random() * 1000) + 1
      const targetIds = Array.from({ length: 5 }, (_, i) => (base < 950 ? base : 950) + i).sort((a, b) => a - b)

      const users = await sql`
        SELECT id, name, balance
        FROM bench_users
        WHERE id = ANY(${targetIds})
        order by id
      `.catch(err => {
        console.log(err)
        return null
      })

      if (!users) {
        return Response.json({ error: 'Error' }, { status: 500 })
      }

      for (const user of users) {
        if (user.id !== targetIds[counter++]) {
          console.log(users, targetIds)

          return Response.json({ error: 'Id mismatch' }, { status: 500 })
        }
      }

      return Response.json(users)
    }

    if (pathname === '/user') {
      const targetId = Math.floor(Math.random() * 1000) + 1

      const user = await sql`
        SELECT id, name, balance FROM bench_users WHERE id = ${targetId}
      `
      .catch(err => {
        console.log(err)
        return null
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
      const [row] = await sql<{ res: 1 }[]>`SELECT 1 as res`
      .catch(err => {
        console.log(err)
        return []
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

console.log(`Postgres.js Server running on http://localhost:${server.port}`)