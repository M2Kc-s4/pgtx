# Pgtx

[![Tests](https://github.com/M2K-5F/pgtx/actions/workflows/tests.yaml/badge.svg)](https://github.com/M2K-5F/pgtx/actions/workflows/tests.yaml)
[![npm version](https://img.shields.io/npm/v/@m2k-5f/pgtx.svg)](https://www.npmjs.com/package/@m2k-5f/pgtx)

A PostgreSQL driver for Node.js with an API that doesn't require a manual to use. Built for regular applications, not for people who need four different flavors of the same stream implementation or a config object with forty optional fields you'll never touch.

```bash
npm install @m2k-5f/pgtx # npm

bun add @m2k-5f/pgtx # bun
```

## Thirty seconds

```typescript
import { sql, Pool } from "@m2k-5f/pgtx"

const pool = new Pool({
  host: 'localhost',
  user: 'postgres',
  password: 'postgres',
  database: 'myapp'
})

const [user] = await pool.query<User>`SELECT * FROM users WHERE id = ${1}`

// returns rows → query, doesn't → execute
await pool.execute`
  INSERT INTO users ${sql.insert<User>([{ name: 'Alice', age: 25 }, { name: 'Bob', age: 30 }])}
`

await pool.begin(async tx => {
  await tx.execute`UPDATE accounts SET balance = balance - 100 WHERE id = ${1}`
  await tx.execute`UPDATE accounts SET balance = balance + 100 WHERE id = ${2}`
})
```

That's most of what you need to know to use it. The rest of this document is for when you want to know *why* it's fast, or you need one of the sharper tools.

## Benchmarks
 
Benchmarks run on GitHub Actions (Ubuntu, 2 vCPUs), reproducible, sources in this repo. Take CI numbers with the usual grain of salt — noisy neighbors and all that — but the gap is wide enough that it holds.
 
**HTTP throughput against `postgres.js` and Bun's own native `Bun.sql` driver, on Bun:**
 
| Connections | Pgtx | Postgres.js | Bun.sql |
|---:|---:|---:|---:|
| 50 | **29,252 req/s** | 14,267 req/s | 17,634 req/s |
| 200 | **31,565 req/s** | 12,316 req/s | 19,441 req/s |
| 500 | **32,091 req/s** | 8,806 req/s | 19,267 req/s |
 
Bun.sql is Bun's own built-in driver, written in native code and generally treated as the speed baseline in that ecosystem. Pgtx stays ahead of it at every concurrency level tested — the gap doesn't come from JS-vs-native, it comes from the protocol implementation.
 
**How:** everything you fire concurrently against the same connection gets folded into one pipelined write — Parse/Bind/Execute for every query in the batch goes out in a single `socket.write()`, and results get demuxed as they come back, in order, without buffering rows you haven't asked for yet. Prepared statements are cached and deduplicated automatically, row descriptions are cached alongside them. None of this requires you to change how you write queries.


## The parts worth knowing about


### Typed, fluent composition

Every query returns a `Future<T, PostgresError>` — a `Promise` subclass from [`fluent-future`](https://www.npmjs.com/package/fluent-future) that keeps the error type attached instead of collapsing it to `unknown`. The same chain handles both paths: transform or act on success, transform or act on failure, without a `try/catch` in sight.

```typescript
const user = await pool.query<User>`
  SELECT * FROM users WHERE id = ${userId}
`
  .map(rows => rows[0])                              // success → new value
  .tap(user => logger.info('loaded user', user.id))  // success → side effect, value untouched
  .andThen(user =>                                   // success → new Future
    pool.query<Post>`SELECT * FROM posts WHERE user_id = ${user.id}`
  )
  .recoverIf(err => err.code === '42P01', [])         // one specific error → fallback
  .mapErr(err => new AppError(err))                   // whatever error is left → transformed
  .tapErr(err => logger.error(err))                   // error → side effect, error untouched
```

`map`, `tap`, and `andThen` never see the error; `mapErr`, `recoverIf`, and `tapErr` never see the success value — each method only touches the side it's named for, so a chain like the one above reads top to bottom without a `try/catch` breaking it up.

---

### Full PostgreSQL type support
 
Pgtx uses PostgreSQL's binary protocol directly, so types aren't passed through as `any` and hoped for — geometric, temporal, and array types decode into real, named shapes:
 
```typescript
import type {
  PgPoint, PgLine, PgLineSegment, PgBox, PgPath, PgPolygon, PgInterval
} from "@m2k-5f/pgtx"
 
const [venue] = await pool.query<{
  id: number
  location: PgPoint
  footprint: PgPolygon
  frontage: PgLineSegment
  openHours: PgInterval
}>`
  SELECT id, location, footprint, frontage, open_hours
  FROM venues
  WHERE id = ${venueId}
`
 
venue.location.x        // number
venue.footprint.points  // PgPoint[]
venue.frontage.a.y      // number
venue.openHours.months  // number
```
 
`int8` and `int8[]` follow the same principle at the config level: set `int8toBigint` on the pool and the values you get back — and the types you write against them — are `bigint` instead of `number`, with no manual casting at the call site:
 
```typescript
const pool = new Pool({ ...config, int8toBigint: true })
 
const [{ total, ids }] = await pool.query<{
  total: bigint
  ids: (bigint | null)[]
}>`
  SELECT sum(amount) AS total, array_agg(id) AS ids FROM ledger
`
```

---

### Pipelining, by default

Queries started in the same tick are folded into one pipelined write automatically — no batching API, no config flag:

```typescript
const users = pool.query<User>`SELECT * FROM users`
const posts = pool.query<Post>`SELECT * FROM posts`

const [usersResult, postsResult] = await Promise.all([users, posts])
```

Both queries go out in a single `socket.write()` and come back demuxed, in order. Whether it's 2 queries or 20, the round trip count doesn't change: one RTT to send the whole batch, one RTT to get every result back.

`fluent-future`'s `Bind` gives the same parallelism a shape suited to independent, differently-typed queries:

```typescript
// 5 queries, 2 round-trips
const { user, posts, ...data } = await Bind({
  user: pool.query<User>`...`,
  config: pool.query<Config>`...`,
  announcements: pool.query<Announcement>`...`
}).bind({
  posts: ({ user }) => pool.query<Post>`...`,
  notifications: ({ user }) => pool.query<Notif>`...`
})
```

`user` and `config` fire together, pipeline together, and resolve together — still 2 RTT total, just with the results already assembled into an object instead of an array you have to destructure by position.

Errors don't leak across a batch, either. If one query in a pipelined group fails — a bad column, a constraint violation — only its own `Future` rejects; the others in the same batch still resolve normally with their own rows. Nothing gets rolled back or aborted on their account, because nothing tied them together in the first place beyond sharing a socket.


## Exstra bits

### Transactions and savepoints

Transactions are explicit and composable:

```typescript
await pool.begin(async tx => {
  await tx.query`UPDATE accounts SET balance = balance - ${amount}`
  await tx.query`UPDATE accounts SET balance = balance + ${amount}`
})
```

Use savepoints when only part of a transaction should be rolled back:

```typescript
await tx.savepoint(async sp => {
  await sp.query`INSERT INTO audit_log ${event}`
})
```
 
For a complete list of supported PostgreSQL types, their JavaScript input/output types, and string formats, see the [PostgreSQL Data Types](./DATATYPES.md) reference.

---

### Streaming that doesn't buffer

`pool.stream()` pipes rows straight from the socket into a `ReadableStream`, no intermediate array, no GC spike from holding a million-row export in memory.

```typescript
for await (const log of pool.stream<Log>`SELECT * FROM application_logs WHERE level = ${'error'}`) {
  console.log(log.timestamp, log.data)
}
```

It's a real Web Streams object, so it drops straight into an HTTP response body:

```typescript
fetch(req) {
  const stream = pool.stream`SELECT id, email FROM giant_user_table`
  return new Response(stream, { headers: { "Content-Type": "application/json" } })
}
```

---

### LISTEN / NOTIFY without babysitting a connection

```typescript
await pool.notify('user_events', JSON.stringify({ id: 42, action: 'signup' }))

const unlisten = await pool.listen('user_events', payload => {
  console.log('got:', payload)
})

// later
await unlisten() // sends UNLISTEN, hands the connection back
```

`pool.listen` borrows a dedicated connection and manages its lifecycle for you. If you need to multiplex several callbacks onto one channel on a connection you're pinning yourself, drop down to `conn.listen`/`conn.unlisten` directly — just don't release that connection back to the pool while you're still using it for that.

---

### Building queries without string-gluing

```typescript
// bulk insert — columns inferred from the object
await pool.execute`INSERT INTO users ${sql.insert(users)}`

// dynamic SET clause
await pool.execute`UPDATE users SET ${sql.update({ status: 'active', last_login: new Date() })} WHERE id = ${userId}`
```

Rule of thumb: `execute` when you don't need rows back, `query` when you do — same rule as the raw driver, `sql.*` doesn't change it.

```typescript
// composable fragments
const filter = sql.fragment`status = ${'active'} AND age > ${21}`
await pool.query`SELECT * FROM users WHERE ${filter}`

// clean WHERE from an object, undefined keys just drop out
await pool.query`SELECT * FROM users WHERE ${sql.where({ role: 'admin', age: undefined, active: true })}`

// conditional fragments
await pool.query`SELECT * FROM posts ${search ? sql.fragment`WHERE title ILIKE ${search}` : sql.empty}`
```

`undefined` means `DEFAULT` in an insert, means "skip this field" in an update, and throws if you try to hand it to `VALUES` or an array — it's meant to be a decision point, not a silent `NULL`.

#### Not doing this

```typescript
// don't
await pool.query(`SELECT * FROM users WHERE name = '${userInput}'`)

// do
await pool.query`SELECT * FROM users WHERE name = ${userInput}`
await pool.query`SELECT * FROM ${sql.ident(tableName)}`
```

Everything that goes through a tagged template is bound as `$1, $2, ...`. There's no code path where a template value becomes raw SQL text — if you need a dynamic identifier or literal, `sql.ident`/`sql.literal` exist precisely so you're never tempted to interpolate by hand.

## API

### `Connection`

```typescript
class Connection {
  static new(config: ConnectionPartialConfig): Future<Connection, PostgresError>

  query<T>(strings: TemplateStringsArray, ...values: any[]): Future<T[], PostgresError>
  execute(strings: TemplateStringsArray, ...values: any[]): Future<void, PostgresError>
  stream<T>(strings: TemplateStringsArray, ...values: any[]): ReadableStream<T>
  begin<T>(callback: (tx: Transaction) => Promise<T>): Future<T, unknown>
  notify(channelName: string, payload?: string): Future<void, PostgresError>
  listen(channelName: string, callback: (payload: string) => void): Future<void, PostgresError>
  unlisten(channelName: string, callback: (payload: string) => void): Future<void, PostgresError>
  close(): Future<void, PostgresError>

  get isOpened(): boolean
  get isClosed(): boolean
}

interface ConnectionPartialConfig {
  user: string
  password?: string
  host: string
  port: number
  database: string
  logLevel?: 'error' | 'notice' | 'query' | "none"   // default 'error'
  int8toBigint?: boolean                     // default false
  queryTimeout?: number                      // default 30000 (ms)
  syncShedule?: 'beforeMicrotask' | 'afterMicrotask' | 'Immediate'  // default 'Immediate'
  ssl?: 'disable' | 'prefer' | 'require' // defaut 'prefer' 
  caPath?: string // forces `ssl` to 'require' if provided
}
```

### `Pool`

```typescript
class Pool {
  constructor(config: PoolPartialConfig)

  query<T>(strings: TemplateStringsArray, ...values: any[]): Future<T[], PostgresError>
  execute(strings: TemplateStringsArray, ...values: any[]): Future<void, PostgresError>
  stream<T>(strings: TemplateStringsArray, ...values: any[]): ReadableStream<T>
  begin<T>(callback: (tx: Transaction) => Promise<T>): Future<T, unknown>
  notify(channelName: string, payload?: string): Future<void, PostgresError>
  listen(channel: string, callback: (payload: string) => void): Future<() => Future<void, PostgresError>, PostgresError>
  withAcquire<T>(fn: (conn: Connection) => Promise<T>): Future<T, unknown>
  acquire(): Future<Connection, PostgresError>
  release(conn: Connection): void
  close(): Future<void, PostgresError>

  get size(): number
  get total(): number
}

interface PoolPartialConfig extends ConnectionPartialConfig {
  max?: number  // default 20
}
```

### `Transaction`

```typescript
class Transaction {
  query<T>(strings: TemplateStringsArray, ...values: any[]): Future<T[], PostgresError>
  commit(): Future<void, PostgresError>
  rollback(): Future<void, PostgresError>
  savepoint<T>(name: string, callback: (tx: Transaction) => Promise<T>): Future<T, unknown>

  get isActive(): boolean
}
```

### `sql`

```typescript
const sql: {
  ident<T extends string>(name: T): IdentifierClause<T>
  literal<T extends string>(value: T): LiteralClause<T>
  fragment(strings: TemplateStringsArray, ...values: any[]): FragmentClause
  insert<T extends Record<string, any>>(...objects: T[]): InsertClause<T>
  update<T extends Record<string, any>>(object: T): UpdateClause<T>
  where<T extends Record<string, any>>(map: T): WhereClause<T>
  excluded(fields: string[]): ExcludeUpdateClause
  array(values: any[], separator?: string): ArrayClause
  empty: EmptyClause
}
```

## What this isn't

Not an ORM. No migrations, no model layer, no query builder that hides SQL from you. You write SQL, Pgtx gets it to Postgres as fast as it can and gets the rows back to you with as little overhead as possible. If you want an ORM on top, Pgtx is a fine thing to put underneath one.

## License

MIT © [M2K-5F](https://github.com/M2K-5F)

---

**Made with ❤️ and a bit of insanity**

*Manufactured under license by the **Blazing Corporation**. Side effects may include throughput.*