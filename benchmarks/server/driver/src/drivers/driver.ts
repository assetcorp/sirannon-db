// The neutral surface every database implements. The harness never knows whether it is driving
// Sirannon or PostgreSQL. It asks a driver to apply a schema, seed rows, and run a read or a
// write, and the driver reaches its database through that database's own shipping client. SQL
// arrives with `?` placeholders; a driver renders the placeholder in its own style.

import type { SeedTable } from '../workloads.ts'

export type Dialect = 'sqlite' | 'postgres'

export abstract class Driver {
  abstract readonly name: string
  abstract readonly delivery: string
  abstract readonly dialect: Dialect

  abstract connect(): Promise<void>
  abstract info(): Promise<Record<string, unknown>>
  abstract executeDdl(statements: string[]): Promise<void>
  abstract dropTables(tables: string[]): Promise<void>
  abstract seed(tables: SeedTable[]): Promise<void>
  abstract read(sql: string, params: unknown[]): Promise<void>
  abstract write(sql: string, params: unknown[]): Promise<void>
  abstract close(): Promise<void>

  render(sql: string): string {
    return sql
  }

  insertSql(table: SeedTable): string {
    const placeholders = table.columns.map(() => '?').join(', ')
    const columns = table.columns.join(', ')
    return `INSERT INTO ${table.table} (${columns}) VALUES (${placeholders})`
  }
}
