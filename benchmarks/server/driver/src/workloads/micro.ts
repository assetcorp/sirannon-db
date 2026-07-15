// The single-statement micro workloads: one point read by primary key, one insert, one update.
// Each operation is one statement and one round trip on both engines, so these are the baseline the
// heavier workloads are read against.

import type { SeededRng } from '../rng.ts'
import type { SeedTable, Workload } from './workload.ts'

const MICRO_SCHEMA =
  'CREATE TABLE IF NOT EXISTS users (' +
  'id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, age INTEGER NOT NULL, bio TEXT)'

const FIRST_NAMES = ['Alice', 'Bob', 'Carol', 'Dave', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy']
const LAST_NAMES = [
  'Smith',
  'Johnson',
  'Williams',
  'Brown',
  'Jones',
  'Garcia',
  'Miller',
  'Davis',
  'Rodriguez',
  'Martinez',
]

export function userRow(rng: SeededRng, rowId: number): unknown[] {
  const name = `${FIRST_NAMES[rowId % FIRST_NAMES.length]} ${LAST_NAMES[rowId % LAST_NAMES.length]}`
  const email = `user${rowId}@example.com`
  const age = 18 + (rowId % 62)
  const bio = `Bio for user ${rowId}: ${rng.text(50)}`
  return [rowId, name, email, age, bio]
}

function* userRows(rng: SeededRng, dataSize: number): Generator<unknown[]> {
  for (let i = 0; i < dataSize; i++) {
    yield userRow(rng, i + 1)
  }
}

function userSeed(rng: SeededRng, dataSize: number): SeedTable[] {
  return [{ table: 'users', columns: ['id', 'name', 'email', 'age', 'bio'], rows: userRows(rng, dataSize) }]
}

function emptySeed(): SeedTable[] {
  return []
}

export function microWorkloads(): Workload[] {
  return [
    {
      name: 'point-select',
      category: 'micro',
      tables: ['users'],
      sqliteSchema: MICRO_SCHEMA,
      postgresSchema: MICRO_SCHEMA,
      seed: userSeed,
      operations: [
        {
          name: 'point-select',
          weight: 1.0,
          kind: 'read',
          sqliteSql: 'SELECT * FROM users WHERE id = ?',
          postgresSql: 'SELECT * FROM users WHERE id = ?',
          params: ctx => [ctx.zipf.next(ctx.rng) + 1],
        },
      ],
    },
    {
      name: 'bulk-insert',
      category: 'micro',
      tables: ['users'],
      sqliteSchema: MICRO_SCHEMA,
      postgresSchema: MICRO_SCHEMA,
      seed: emptySeed,
      operations: [
        {
          name: 'bulk-insert',
          weight: 1.0,
          kind: 'write',
          sqliteSql: 'INSERT OR REPLACE INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)',
          postgresSql:
            'INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?) ' +
            'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name',
          params: ctx => userRow(ctx.rng, ctx.rng.below(ctx.dataSize) + 1),
        },
      ],
    },
    {
      name: 'batch-update',
      category: 'micro',
      tables: ['users'],
      sqliteSchema: MICRO_SCHEMA,
      postgresSchema: MICRO_SCHEMA,
      seed: userSeed,
      operations: [
        {
          name: 'batch-update',
          weight: 1.0,
          kind: 'write',
          sqliteSql: 'UPDATE users SET age = ? WHERE id = ?',
          postgresSql: 'UPDATE users SET age = ? WHERE id = ?',
          params: ctx => [ctx.rng.below(80) + 18, ctx.zipf.next(ctx.rng) + 1],
        },
      ],
    },
  ]
}
