import { Pool as PgtxPool } from "./pool";
import {
    IdentifierClause,
    InsertClause,
    ArrayClause,
    UpdateClause,
    ExcludeUpdateClause,
    emptyClause,
    WhereClause,
    LiteralClause,
    FragmentClause,
} from './clauses'
import { Connection } from "./connection";
import { Transaction } from "./transaction";


export type PgPoint = { x: number, y: number }
export type PgLine = { a: number, b: number, c: number }
export type PgLineSegment = { a: PgPoint, b: PgPoint }
export type PgBox = { high: PgPoint, low: PgPoint }
export type PgPath = { closed: boolean, points: PgPoint[] }
export type PgPolygon = { points: PgPoint[] }
export type PgInterval = { months: number, days: number, microseconds: number }

/**
 * Core SQL tagging utility for Pgtx.
 * Provides type-safe helpers for building dynamic queries with recursive support.
 */
export const sql = {
    /** 
     * Creates a VALUES clause for INSERT queries.
     * Supports single objects and arrays of objects.
     * 
     * @example 
     * sql.insert({ name: 'Ivan', age: 25 }) 
     * // Result: (name, age) VALUES ($1, $2)
     * 
     * @example 
     * sql.insert([{ id: 1 }, { id: 2 }]) 
     * // Result: (id) VALUES ($1), ($2)
     */
    insert: InsertClause.create,

    /** 
     * Generates a SET clause for UPDATE queries from a JavaScript object.
     * 
     * @example 
     * sql.update({ status: 'active', updated_at: new Date() }) 
     * // Result: status = $1, updated_at = $2
     */
    update: UpdateClause.create,

    /** 
     * Generates an assignment list for ON CONFLICT DO UPDATE using the EXCLUDED table.
     * 
     * @example 
     * sql`INSERT INTO users ${sql.insert(data)} ON CONFLICT (id) DO UPDATE SET ${sql.excluded(['name', 'email'])}`
     * // Result: name = EXCLUDED.name, email = EXCLUDED.email
     */
    excluded: ExcludeUpdateClause.create,
    
    /** 
     * Represents a safe empty SQL fragment.
     * Useful for dynamic query building when a condition or list might be optional.
     * 
     * @example 
     * const filters = [];
     * sql`SELECT * FROM users ${filters.length ? sql.fragment`WHERE ...` : sql.empty}`
     * // Result: SELECT * FROM users
     */
    empty: emptyClause,

    /**
   * Generates a list of conditions for a WHERE clause from a JavaScript object.
   * Works similarly to sql.update, but uses ' AND ' as a separator instead of a comma.
   *
   * @example
   * sql`SELECT * FROM users WHERE ${sql.where({ active: true, role: 'admin' })}`
   * // Result: active = $1 AND role = $2
   *
   * @example
   * sql`DELETE FROM tasks WHERE ${sql.where({ id: 10, user_id: 5 })}`
   * // Result: id = $1 AND user_id = $2
   */
    where: WhereClause.create,    

    /** 
     * Safely escapes SQL identifiers (table or column names) using double quotes.
     * 
     * @example 
     * sql.ident('users') 
     * // Result: "users"
     * 
     * @example 
     * sql.ident('table.column') 
     * // Result: "table.column"
     */
    ident: IdentifierClause.create,

    /** 
     * Injects raw, unescaped SQL strings. 
     * ⚠️ Use with caution to prevent SQL injection!
     * 
     * @example 
     * sql.literal('DESC') 
     * // Result: DESC
     */
    literal: LiteralClause.create,

    /** 
     * Creates a reusable, recursive SQL fragment.
     * Fragments can be nested within each other; argument numbering is handled automatically.
     * 
     * @example 
     * const filter = sql.fragment`age > ${18}`;
     * sql`SELECT * FROM users WHERE ${filter} AND status = ${'active'}`
     * // Result: SELECT * FROM users WHERE age > $1 AND status = $2
     */
    fragment: FragmentClause.create,

    /** 
     * Formats an array for dynamic lists (IN clauses, column lists, or joined conditions).
     * Supports recursive Clauses (fragments, idents) within the array.
     * 
     * @example 
     * // Case A: IN clause (manual brackets)
     * sql`WHERE id IN (${sql.array([1, 2])})`
     * // Result: WHERE id IN ($1, $2)
     * 
     * @example 
     * // Case B: Dynamic WHERE conditions
     * const conds = [sql.fragment`a = 1`, sql.fragment`b = ${2}`];
     * sql`WHERE ${sql.array(conds, ' AND ')}`
     * // Result: WHERE a = 1 AND b = $1
     * 
     * @example 
     * // Case C: Column list
     * sql`SELECT ${sql.array([sql.ident('id'), sql.ident('name')])}`
     * // Result: SELECT "id", "name"
     */
    array: ArrayClause.create,
}


/**
 * Main Pgtx Connection Pool. 
 * Manages connections, transactions (including SAVEPOINTs), and prepared statements.
 */

export { Connection as Connection }

export { Transaction as Transaction }

export { PgtxPool as Pool }


export * from "./clauses"
export * from './error'
