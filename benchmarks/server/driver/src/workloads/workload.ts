// The shape every workload in the catalogue takes. A workload names its schema for each dialect, a
// seed that fills the tables, and a weighted set of operations. Operations carry SQL for both
// dialects written with `?` placeholders; each driver renders the placeholder in its own style.
//
// An operation is one unit of offered load and one entry in the reported ops/sec, so the kinds
// deliberately differ in what they cost. A read or a write is one statement; a read-modify-write is
// a read then a dependent write; a transaction is a statement list that commits or rolls back as a
// unit. What each kind costs is disclosed per workload rather than left for a reader to assume,
// because ops/sec means nothing without it.

import type { SeededRng, ZipfianGenerator } from '../rng.ts'

export interface OperationContext {
  rng: SeededRng
  zipf: ZipfianGenerator
  dataSize: number
}

export type OperationKind = 'read' | 'write' | 'rmw' | 'transaction'

export interface StatementTemplate {
  sqliteSql: string
  postgresSql: string
}

interface OperationBase {
  name: string
  weight: number
}

export interface ReadOperation extends OperationBase {
  kind: 'read'
  sqliteSql: string
  postgresSql: string
  params: (ctx: OperationContext) => unknown[]
}

export interface WriteOperation extends OperationBase {
  kind: 'write'
  sqliteSql: string
  postgresSql: string
  params: (ctx: OperationContext) => unknown[]
}

export interface ReadModifyWriteOperation extends OperationBase {
  kind: 'rmw'
  readSqliteSql: string
  readPostgresSql: string
  writeSqliteSql: string
  writePostgresSql: string
  params: (ctx: OperationContext) => { read: unknown[]; write: unknown[] }
}

// The statement list is fixed and the params for the whole list are drawn together, so values that
// must agree across statements (an order total and the balance it charges) are drawn once.
export interface TransactionOperation extends OperationBase {
  kind: 'transaction'
  statements: StatementTemplate[]
  params: (ctx: OperationContext) => unknown[][]
}

export type Operation = ReadOperation | WriteOperation | ReadModifyWriteOperation | TransactionOperation

export interface SeedTable {
  table: string
  columns: string[]
  rows: Iterable<unknown[]>
}

export interface Workload {
  name: string
  category: string
  tables: string[]
  sqliteSchema: string
  postgresSchema: string
  seed: (rng: SeededRng, dataSize: number) => SeedTable[]
  operations: Operation[]
}

export function statementsPerOperation(operation: Operation): number {
  switch (operation.kind) {
    case 'transaction':
      return operation.statements.length
    case 'rmw':
      return 2
    default:
      return 1
  }
}

export function pickWeighted<T extends { weight: number }>(rng: SeededRng, items: T[]): T {
  const draw = rng.fraction()
  let cumulative = 0.0
  for (const item of items) {
    cumulative += item.weight
    if (draw < cumulative) {
      return item
    }
  }
  const last = items[items.length - 1]
  if (last === undefined) {
    throw new Error('a workload must declare at least one operation')
  }
  return last
}
