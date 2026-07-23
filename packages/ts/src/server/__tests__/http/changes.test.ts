import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { computeChecksum } from '../../../core/sync/checksum.js'
import { HLC } from '../../../core/sync/hlc.js'
import type { ReplicationChange } from '../../../core/sync/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

const DEVICE = 'dddd0000dddd0000dddd0000dddd0000'

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-changes-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

function deviceChanges(): ReplicationChange[] {
  return [
    {
      table: 'notes',
      operation: 'insert',
      rowId: '10',
      primaryKey: { id: 10 },
      hlc: new HLC(DEVICE).now(),
      txId: 'device-tx-1',
      nodeId: DEVICE,
      newData: { id: 10, body: 'pushed' },
      oldData: null,
    },
  ]
}

function wireBatch(changes: ReplicationChange[]): Record<string, unknown> {
  return {
    sourceNodeId: DEVICE,
    batchId: `${DEVICE}-1-1`,
    fromSeq: '1',
    toSeq: '1',
    hlcRange: { min: changes[0].hlc, max: changes[changes.length - 1].hlc },
    changes,
    checksum: computeChecksum(changes),
  }
}

async function postChanges(body: unknown): Promise<Response> {
  return fetch(`${baseUrl}/db/test/changes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

describe('POST /db/:id/changes', () => {
  it('applies a valid device batch', async () => {
    const res = await postChanges({ batch: wireBatch(deviceChanges()) })
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ applied: 1, skipped: 0, conflicts: 0 })

    const db = sirannon.get('test')
    const rows = await db?.query<{ body: string }>('SELECT body FROM notes WHERE id = 10')
    expect(rows).toEqual([{ body: 'pushed' }])
  })

  it('reports an already-applied batch as skipped', async () => {
    const batch = wireBatch(deviceChanges())
    await postChanges({ batch })
    const res = await postChanges({ batch })
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ applied: 0, skipped: 1, conflicts: 0 })
  })

  it('rejects a structurally invalid batch', async () => {
    const res = await postChanges({ batch: { sourceNodeId: 'nope' } })
    expect(res.status).toBe(400)
    const body = (await res.json()) as { error: { code: string } }
    expect(body.error.code).toBe('INVALID_REQUEST')
  })

  it('rejects ddl operations', async () => {
    const changes = deviceChanges()
    changes[0] = { ...changes[0], operation: 'ddl' as ReplicationChange['operation'] }
    const res = await postChanges({ batch: wireBatch(changes) })
    expect(res.status).toBe(400)
  })

  it('rejects a checksum mismatch', async () => {
    const batch = wireBatch(deviceChanges())
    batch.checksum = 'f'.repeat(64)
    const res = await postChanges({ batch })
    expect(res.status).toBe(400)
    const body = (await res.json()) as { error: { code: string } }
    expect(body.error.code).toBe('BATCH_VALIDATION_ERROR')
  })

  it('rejects changes whose nodeId differs from the source', async () => {
    const changes = deviceChanges()
    changes[0] = { ...changes[0], nodeId: 'eeee0000eeee0000eeee0000eeee0000' }
    const res = await postChanges({ batch: wireBatch(changes) })
    expect(res.status).toBe(400)
  })
})
