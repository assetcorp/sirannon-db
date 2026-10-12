import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { QueryError } from '../../../core/errors.js'
import {
  armControlStatementFault,
  type ControlStatementFault,
  type ControlStatementFaultConfig,
  controlStatementFaultArmed,
  controlStatementFaultDriver,
} from '../../helpers/fault-drivers/control-statement-fault-driver.js'
import { columnRaw, createWorkspace, type Workspace, WRITER_PLACEMENTS } from './harness.js'

const INSERT_PARCEL = 'INSERT INTO parcels (label) VALUES (?)'

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('commit-failures')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(WRITER_PLACEMENTS)('grouped writes whose control statements fail, with $label', ({ writerWorker }) => {
  async function openParcels(fault: ControlStatementFault): Promise<{
    db: Database
    path: string
    config: ControlStatementFaultConfig
  }> {
    const config = { fault, armFile: workspace.path(`${fault}.armed`) }
    const path = workspace.path('parcels.db')
    const db = await workspace
      .registry({ driver: controlStatementFaultDriver(config) })
      .open('parcels', path, { writerWorker })
    await db.execute('CREATE TABLE parcels (id INTEGER PRIMARY KEY, label TEXT NOT NULL UNIQUE)')
    await db.execute(INSERT_PARCEL, ['seeded'])
    return { db, path, config }
  }

  it('fails every caller and applies their writes exactly once when SQLite applies the commit and then reports an error', async () => {
    const { db, path, config } = await openParcels('commit-applies-then-fails')
    armControlStatementFault(config)

    const settled = await Promise.allSettled([
      db.execute(INSERT_PARCEL, ['north']),
      db.execute(INSERT_PARCEL, ['south']),
      db.executeTransaction([
        { sql: INSERT_PARCEL, params: ['east'] },
        { sql: INSERT_PARCEL, params: ['west'] },
      ]),
    ])
    expect(controlStatementFaultArmed(config)).toBe(false)
    expect(settled.map(outcome => (outcome.status === 'rejected' ? outcome.reason.code : 'ok'))).toEqual([
      'TRANSACTION_ERROR',
      'TRANSACTION_ERROR',
      'TRANSACTION_ERROR',
    ])

    armControlStatementFault(config)
    await expect(
      db.transaction(async tx => {
        await tx.execute(INSERT_PARCEL, ['inside-callback'])
      }),
    ).rejects.toMatchObject({ code: 'TRANSACTION_ERROR' })
    expect(controlStatementFaultArmed(config)).toBe(false)

    await expect(db.execute(INSERT_PARCEL, ['after-the-fault'])).resolves.toMatchObject({ changes: 1 })
    expect(columnRaw(path, 'SELECT label FROM parcels ORDER BY id')).toEqual([
      'seeded',
      'north',
      'south',
      'east',
      'west',
      'inside-callback',
      'after-the-fault',
    ])
  })

  it('rejects the write whose savepoint SQLite refuses, naming that statement, and settles each neighbour on its own', async () => {
    const { db, path, config } = await openParcels('savepoint-refused')
    armControlStatementFault(config)

    const [refused, duplicate, valid] = await Promise.allSettled([
      db.execute(INSERT_PARCEL, ['refused-savepoint']),
      db.execute(INSERT_PARCEL, ['seeded']),
      db.execute(INSERT_PARCEL, ['committed-alone']),
    ])

    expect(controlStatementFaultArmed(config)).toBe(false)
    expect(refused.status).toBe('rejected')
    expect(refused.status === 'rejected' && refused.reason).toBeInstanceOf(QueryError)
    expect(refused).toMatchObject({ reason: { code: 'QUERY_ERROR', sql: expect.stringMatching(/^SAVEPOINT\b/) } })
    expect(duplicate).toMatchObject({ status: 'rejected', reason: { code: 'QUERY_ERROR', sql: INSERT_PARCEL } })
    expect(valid).toMatchObject({ status: 'fulfilled', value: { changes: 1 } })
    expect(columnRaw(path, 'SELECT label FROM parcels ORDER BY id')).toEqual(['seeded', 'committed-alone'])
  })

  it('settles every write of a group whose first BEGIN SQLite refuses', async () => {
    const { db, path, config } = await openParcels('begin-refused')
    armControlStatementFault(config)

    const settled = await Promise.allSettled([
      db.execute(INSERT_PARCEL, ['first']),
      db.execute(INSERT_PARCEL, ['seeded']),
      db.executeTransaction([
        { sql: INSERT_PARCEL, params: ['second'] },
        { sql: INSERT_PARCEL, params: ['third'] },
      ]),
    ])

    expect(controlStatementFaultArmed(config)).toBe(false)
    expect(settled.map(outcome => (outcome.status === 'rejected' ? outcome.reason.code : 'ok'))).toEqual([
      'ok',
      'QUERY_ERROR',
      'ok',
    ])
    expect(columnRaw(path, 'SELECT label FROM parcels ORDER BY id')).toEqual(['seeded', 'first', 'second', 'third'])
  })
})
