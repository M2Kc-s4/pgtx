# PostgreSQL Data Types

| PostgreSQL type                        | js Input                                                           | js Output                                                | Schema / Format                        |
| -------------------------------------- | ------------------------------------------------------------------ | -------------------------------------------------------- | -------------------------------------- |
| `bool`                                 | `boolean`                                                          | `boolean`                                                |                                        |
| `text` / `varchar` / `char` / `bpchar` | `string`                                                           | `string`                                                 |                                        |
| `name`                                 | `string`                                                           | `string`                                                 |                                        |
| `int2`                                 | `number`                                                           | `number`                                                 |                                        |
| `int4`                                 | `number`                                                           | `number`                                                 |                                        |
| `int8`                                 | `bigint` / `number`                                                | `bigint` / `number`*                                     |                                        |
| `float4` / `float8`                    | `number`                                                           | `number`                                                 |                                        |
| `bytea`                                | `Buffer` / `Uint8Array`                                            | `Uint8Array`                                             |                                        |
| `timestamp` / `timestamptz`            | `Date`                                                             | `Date`                                                   |                                        |
| `date`                                 | `Date`                                                             | `Date`                                                   |                                        |
| `time`                                 | `string`                                                           | `string`                                                 | `HH:MM:SS.mmm`                         |
| `timetz`                               | `string`                                                           | `string`                                                 | `HH:MM:SS.mmm±HH:MM`                   |
| `interval`                             | `{ months: number, days: number, microseconds: number }` | `{ months: number, days: number, microseconds: number }` |                                        |
| `json` / `jsonb`                       | `unknown`                                                          | `unknown`                                                |                                        |
| `uuid`                                 | `string`                                                           | `string`                                                 | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `inet` / `cidr`                        | `string`                                                           | `string`                                                 | IPv4 / IPv6 address or CIDR            |
| `macaddr`                              | `string`                                                           | `string`                                                 | `xx:xx:xx:xx:xx:xx`                    |
| `oid` / `xid` / `cid` / `regproc`      | `number`                                                           | `number`                                                 |                                        |
| `point`                                | `Point`                                                            | `Point`                                                  | `{ x: number, y: number }`             |
| `lseg`                                 | `{ a: Point, b: Point }`                                           | `{ a: Point, b: Point }`                                 |                                        |
| `path`                                 | `{ closed: boolean, points: Point[] }`                             | `{ closed: boolean, points: Point[] }`                   |                                        |
| `box`                                  | `{ high: Point, low: Point }`                                      | `{ high: Point, low: Point }`                            |                                        |
| `polygon`                              | `{ points: Point[] }`                                              | `{ points: Point[] }`                                    |                                        |
| `line`                                 | `{ a: number, b: number, c: number }`                              | `{ a: number, b: number, c: number }`                    |                                        |
| `bool[]`                               | `(boolean \| null)[]`                                              | `(boolean \| null)[]`                                    |                                        |
| `int2[]`                               | `(number \| null)[]`                                               | `(number \| null)[]`                                     |                                        |
| `int4[]`                               | `(number \| null)[]`                                               | `(number \| null)[]`                                     |                                        |
| `int8[]`                               | `(number \| bigint \| null)[]`                                     | `(number \| null)[] \| (bigint \| null)[]`*                          |                                        |
| `text[]` / `varchar[]`                 | `(string \| null)[]`                                               | `(string \| null)[]`                                     |                                        |
| `json[]` / `jsonb[]`                   | `unknown[]`                                                        | `unknown[]`                                              |                                        |
| `uuid[]`                               | `(string \| null)[]`                                               | `(string \| null)[]`                                     | UUID format                            |
| `numeric`                              | `string`                                                 | `string`                                                 | `[-]digits[.digits]`                   |
| `numeric[]`                            | `(string \| null)[]`                                     | `(string \| null)[]`                                     | `[-]digits[.digits]`                   |

`Point` is `{ x: number, y: number }`.

* `int8` output depends on the `int8toBigint` configuration option.
