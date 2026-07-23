import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { CHANGES_TABLE } from '../../internal-tables.js'
import { computeChecksum } from '../../sync/checksum.js'
import { HLC } from '../../sync/hlc.js'
import type { ReplicationBatch, ReplicationChange } from '../../sync/types.js'
import type { ChangeEvent } from '../../types.js'
import { testDriver } from '../helpers/test-driver.js'

const DEVICE = 'dddd0000dddd0000dddd0000dddd0000'

let tempDir: string
let db: Database | undefined

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-apply-'))
})

afterEach(async () => {
  await db?.close()
  db = undefined
  rmSync(tempDir, { recursive: true, force: true })
})

async function openWatched(name: string): Promise<Database> {
  const database = await Database.create('apply', join(tempDir, name), testDriver)
  await database.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await database.watch('notes')
  return database
}

function deviceChange(overrides: Partial<ReplicationChange>): ReplicationChange {
  return {
    table: 'notes',
    operation: 'insert',
    rowId: '10',
    primaryKey: { id: 10 },
    hlc: new HLC(DEVICE).now(),
    txId: 'device-tx-1',
    nodeId: DEVICE,
    newData: { id: 10, body: 'from device' },
    oldData: null,
    ...overrides,
  }
}

function buildBatch(changes: ReplicationChange[], fromSeq = 1n): ReplicationBatch {
  const hlcs = changes.map(c => c.hlc).sort()
  return {
    sourceNodeId: DEVICE,
    batchId: `${DEVICE}-${fromSeq}`,
    fromSeq,
    toSeq: fromSeq + BigInt(changes.length - 1),
    hlcRange: { min: hlcs[0], max: hlcs[hlcs.length - 1] },
    changes,
    checksum: computeChecksum(changes),
  }
}

describe('Database.applyChanges', () => {
  it('applies a device batch and stamps the echo with the device identity', async () => {
    db = await openWatched('apply.db')
    const result = await db.applyChanges(buildBatch([deviceChange({})]))
    expect(result.applied).toBe(1)

    const rows = await db.query<{ body: string }>('SELECT body FROM notes WHERE id = 10')
    expect(rows).toEqual([{ body: 'from device' }])

    await db.close()
    const inspect = await testDriver.open(join(tempDir, 'apply.db'))
    const stmt = await inspect.prepare(`SELECT node_id, tx_id FROM ${CHANGES_TABLE} ORDER BY seq`)
    const changeRows = (await stmt.all()) as { node_id: string; tx_id: string }[]
    await inspect.close()
    db = undefined
    expect(changeRows).toHaveLength(1)
    expect(changeRows[0].node_id).toBe(DEVICE)
    expect(changeRows[0].tx_id).toBe('device-tx-1')
  })

  it('skips a batch that was already applied', async () => {
    db = await openWatched('idempotent.db')
    const batch = buildBatch([deviceChange({})])
    await db.applyChanges(batch)
    const second = await db.applyChanges(batch)
    expect(second.applied).toBe(0)
    expect(second.skipped).toBe(1)

    const rows = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM notes')
    expect(rows[0].count).toBe(1)
  })

  it('rejects a batch whose checksum does not match', async () => {
    db = await openWatched('checksum.db')
    const batch = buildBatch([deviceChange({})])
    await expect(db.applyChanges({ ...batch, checksum: 'tampered' })).rejects.toThrow('Checksum mismatch')
  })

  it('resolves conflicting updates by newest HLC by default', async () => {
    db = await openWatched('conflict.db')
    await db.execute("INSERT INTO notes (id, body) VALUES (10, 'local original')")

    const staleHlc = `${'0'.repeat(12)}-0000-${DEVICE}`
    const stale = deviceChange({
      operation: 'update',
      hlc: staleHlc,
      newData: { id: 10, body: 'stale remote' },
      oldData: { id: 10, body: 'local original' },
    })
    const result = await db.applyChanges(buildBatch([stale]))
    expect(result.conflicts).toBe(1)

    const rows = await db.query<{ body: string }>('SELECT body FROM notes WHERE id = 10')
    expect(rows).toEqual([{ body: 'local original' }])
  })

  it('emits subscription events carrying the device origin', async () => {
    db = await openWatched('events.db')
    const events: ChangeEvent[] = []
    db.on('notes').subscribe(event => events.push(event))

    await db.applyChanges(buildBatch([deviceChange({})]))
    await new Promise(resolve => setTimeout(resolve, 150))

    expect(events.length).toBeGreaterThanOrEqual(1)
    expect(events[0].origin).toBe(DEVICE)
    expect(events[0].row).toEqual({ id: 10, body: 'from device' })
  })
})
