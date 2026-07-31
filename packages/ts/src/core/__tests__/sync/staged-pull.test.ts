import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { Database } from '../../database.js'
import type { DeviceSyncPort } from '../../database-sync.js'
import { STAGED_CHANGES_TABLE } from '../../internal-tables.js'
import { Sirannon } from '../../sirannon.js'
import type { ChangeEvent } from '../../types.js'

const driver = betterSqlite3()
const SERVER_NODE = 'f'.repeat(32)

let tempDir: string
let sirannon: Sirannon
let db: Database
let dbPath: string
let port: DeviceSyncPort

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-staged-'))
  dbPath = join(tempDir, 'device.db')
  sirannon = new Sirannon({ driver })
  db = await sirannon.open('appdb', dbPath)
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')
  port = db.deviceSync()
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

function pulled(
  seq: number,
  id: number,
  body: string,
  txId: string,
  options?: { txEnd?: boolean; table?: string },
): ChangeEvent {
  return {
    type: 'insert',
    table: options?.table ?? 'notes',
    row: { id, body },
    seq: BigInt(seq),
    timestamp: seq,
    rowId: String(id),
    txId,
    origin: SERVER_NODE,
    hlc: `000000000${seq}:0:${SERVER_NODE}`,
    ...(options?.txEnd === true ? { txEnd: true } : {}),
  }
}

async function stagedCount(): Promise<number> {
  const inspect = await driver.open(dbPath)
  try {
    const stmt = await inspect.prepare(`SELECT COUNT(*) AS n FROM ${STAGED_CHANGES_TABLE}`)
    const row = (await stmt.get()) as { n: number | bigint }
    return Number(row.n)
  } finally {
    await inspect.close()
  }
}

async function noteIds(): Promise<number[]> {
  const rows = await db.query<{ id: number }>('SELECT id FROM notes ORDER BY id')
  return rows.map(row => row.id)
}

describe('staged pull', () => {
  it('applies a transaction only when its end is staged, in one atomic step', async () => {
    await port.stagePulledChanges([pulled(1, 1, 'one', 'tx-1'), pulled(2, 2, 'two', 'tx-1')])
    expect(await port.applyStagedPull()).toBeNull()
    expect(await noteIds()).toEqual([])
    expect(await stagedCount()).toBe(2)

    await port.stagePulledChanges([pulled(3, 3, 'three', 'tx-1', { txEnd: true })])
    const applied = await port.applyStagedPull()

    expect(applied).toBe(3n)
    expect(await noteIds()).toEqual([1, 2, 3])
    expect(await stagedCount()).toBe(0)
    expect((await port.getPullState())?.seq).toBe(3n)
  })

  it('rolls back a failing apply, keeps the staged rows, and applies cleanly on retry', async () => {
    await port.stagePulledChanges([
      pulled(1, 1, 'kept back', 'tx-1'),
      pulled(2, 9, 'missing table', 'tx-1', { table: 'ghost', txEnd: true }),
    ])

    await expect(port.applyStagedPull()).rejects.toThrow()
    expect(await noteIds()).toEqual([])
    expect(await stagedCount()).toBe(2)
    expect(await port.getPullState()).toBeNull()

    await db.execute('CREATE TABLE ghost (id INTEGER PRIMARY KEY, body TEXT)')
    expect(await port.applyStagedPull()).toBe(2n)
    expect(await noteIds()).toEqual([1])
    expect(await db.query('SELECT id FROM ghost WHERE id = 9')).toHaveLength(1)
    expect(await stagedCount()).toBe(0)
  })

  it('recovery applies a complete transaction left staged by a crash before apply', async () => {
    await port.stagePulledChanges([pulled(1, 1, 'one', 'tx-1'), pulled(2, 2, 'two', 'tx-1', { txEnd: true })])

    const recovered = await port.recoverStagedPull()

    expect(recovered.resumeSeq).toBe(2n)
    expect(recovered.appliedSeq).toBe(2n)
    expect(recovered.applyError).toBeNull()
    expect(await noteIds()).toEqual([1, 2])
    expect(await stagedCount()).toBe(0)
  })

  it('recovery reports changes staged before a crash through onChange when it applies them', async () => {
    await port.stagePulledChanges([pulled(1, 1, 'one', 'tx-1'), pulled(2, 2, 'two', 'tx-1', { txEnd: true })])

    const seen: ChangeEvent[] = []
    const recovered = await port.recoverStagedPull(undefined, event => seen.push(event))

    expect(recovered.appliedSeq).toBe(2n)
    expect(seen.map(event => Number(event.seq))).toEqual([1, 2])
    expect(seen[1].txEnd).toBe(true)
    expect(seen[0].origin).toBe(SERVER_NODE)
  })

  it('recovery keeps an incomplete tail and resumes from its highest staged sequence', async () => {
    await port.stagePulledChanges([
      pulled(1, 1, 'one', 'tx-1', { txEnd: true }),
      pulled(2, 2, 'two', 'tx-2'),
      pulled(3, 3, 'three', 'tx-2'),
    ])

    const recovered = await port.recoverStagedPull()

    expect(recovered.resumeSeq).toBe(3n)
    expect(recovered.appliedSeq).toBe(1n)
    expect(await noteIds()).toEqual([1])
    expect(await stagedCount()).toBe(2)
  })

  it('recovery drops staged rows the pull cursor already covers without re-applying them', async () => {
    await port.stagePulledChanges([pulled(1, 1, 'one', 'tx-1', { txEnd: true })])
    await port.applyStagedPull()

    await port.stagePulledChanges([pulled(1, 1, 'one', 'tx-1', { txEnd: true })])
    const replayed: ChangeEvent[] = []
    const recovered = await port.recoverStagedPull(undefined, event => replayed.push(event))

    expect(recovered.resumeSeq).toBe(1n)
    expect(replayed).toEqual([])
    expect(await stagedCount()).toBe(0)
    expect(await noteIds()).toEqual([1])
  })

  it('reports each applied change through onChange after the transaction commits', async () => {
    await port.stagePulledChanges([pulled(1, 1, 'one', 'tx-1'), pulled(2, 2, 'two', 'tx-1', { txEnd: true })])

    const seen: ChangeEvent[] = []
    await port.applyStagedPull(undefined, event => seen.push(event))

    expect(seen.map(event => Number(event.seq))).toEqual([1, 2])
    expect(seen[1].txEnd).toBe(true)
    expect(seen[0].origin).toBe(SERVER_NODE)
    expect(seen[0].row).toEqual({ id: 1, body: 'one' })
  })

  it('interleaves staging, apply, and local writes without a deadlock or a lost write', async () => {
    await Promise.all([
      port.stagePulledChanges([pulled(1, 1, 'pulled', 'tx-1', { txEnd: true })]),
      db.execute("INSERT INTO notes (id, body) VALUES (50, 'local during staging')"),
      port.applyStagedPull(),
    ])
    await port.applyStagedPull()

    expect(await noteIds()).toEqual([1, 50])
    expect(await stagedCount()).toBe(0)
  })
})
