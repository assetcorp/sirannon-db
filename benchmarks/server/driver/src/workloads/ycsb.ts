import type { SeededRng } from '../rng.ts'
import type { Operation, OperationContext, SeedTable, Workload } from './workload.ts'

const YCSB_SCHEMA =
  'CREATE TABLE IF NOT EXISTS usertable (' +
  'ycsb_key TEXT PRIMARY KEY, ' +
  'field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT, ' +
  'field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT)'

const YCSB_READ = 'SELECT * FROM usertable WHERE ycsb_key = ?'
const YCSB_UPDATE = 'UPDATE usertable SET field0 = ? WHERE ycsb_key = ?'

function ycsbRow(rng: SeededRng, key: string): unknown[] {
  const fields: string[] = []
  for (let i = 0; i < 10; i++) {
    fields.push(rng.text(100))
  }
  return [key, ...fields]
}

function* ycsbRows(rng: SeededRng, dataSize: number): Generator<unknown[]> {
  for (let i = 0; i < dataSize; i++) {
    yield ycsbRow(rng, `user${i}`)
  }
}

function ycsbSeed(rng: SeededRng, dataSize: number): SeedTable[] {
  const columns = ['ycsb_key']
  for (let i = 0; i < 10; i++) {
    columns.push(`field${i}`)
  }
  return [{ table: 'usertable', columns, rows: ycsbRows(rng, dataSize) }]
}

function ycsbKey(ctx: OperationContext): string {
  return `user${ctx.zipf.next(ctx.rng)}`
}

function ycsbValue(ctx: OperationContext): string {
  return `value_${ctx.rng.below(1_000_000)}`
}

function ycsbReadOp(weight: number): Operation {
  return {
    name: 'read',
    weight,
    kind: 'read',
    sqliteSql: YCSB_READ,
    postgresSql: YCSB_READ,
    params: ctx => [ycsbKey(ctx)],
  }
}

function ycsbUpdateOp(weight: number): Operation {
  return {
    name: 'update',
    weight,
    kind: 'write',
    sqliteSql: YCSB_UPDATE,
    postgresSql: YCSB_UPDATE,
    params: ctx => [ycsbValue(ctx), ycsbKey(ctx)],
  }
}

function ycsbRmwOp(weight: number): Operation {
  return {
    name: 'read-modify-write',
    weight,
    kind: 'rmw',
    readSqliteSql: YCSB_READ,
    readPostgresSql: YCSB_READ,
    writeSqliteSql: YCSB_UPDATE,
    writePostgresSql: YCSB_UPDATE,
    params: ctx => {
      const key = ycsbKey(ctx)
      return { read: [key], write: [ycsbValue(ctx), key] }
    },
  }
}

function ycsbWorkload(name: string, operations: Operation[]): Workload {
  return {
    name,
    category: 'ycsb',
    tables: ['usertable'],
    sqliteSchema: YCSB_SCHEMA,
    postgresSchema: YCSB_SCHEMA,
    seed: ycsbSeed,
    operations,
  }
}

export function ycsbWorkloads(): Workload[] {
  return [
    ycsbWorkload('ycsb-a', [ycsbReadOp(0.5), ycsbUpdateOp(0.5)]),
    ycsbWorkload('ycsb-b', [ycsbReadOp(0.95), ycsbUpdateOp(0.05)]),
    ycsbWorkload('ycsb-c', [ycsbReadOp(1.0)]),
    ycsbWorkload('ycsb-f', [ycsbReadOp(0.5), ycsbRmwOp(0.5)]),
  ]
}
