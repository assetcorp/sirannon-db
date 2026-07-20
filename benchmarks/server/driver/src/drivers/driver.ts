import type { FailureClassifier } from '../failures.ts'
import type { SeedTable } from '../workloads/workload.ts'

export type Dialect = 'sqlite' | 'postgres'

export interface TransactionStatement {
  sql: string
  params: unknown[]
}

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
  abstract transaction(statements: TransactionStatement[]): Promise<void>
  abstract transactionRoundTrips(statementCount: number): number
  abstract close(): Promise<void>

  abstract readonly failureClassifier: FailureClassifier

  render(sql: string): string {
    return sql
  }

  insertSql(table: SeedTable): string {
    const placeholders = table.columns.map(() => '?').join(', ')
    const columns = table.columns.join(', ')
    return `INSERT INTO ${table.table} (${columns}) VALUES (${placeholders})`
  }
}
