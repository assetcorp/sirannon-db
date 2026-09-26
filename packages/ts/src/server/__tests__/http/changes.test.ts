import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { HookDeniedError, RequestDeniedError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import { computeChecksum } from '../../../core/sync/checksum.js'
import { HLC } from '../../../core/sync/hlc.js'
import type { ReplicationChange } from '../../../core/sync/types.js'
import type { HookConfig } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

const DEVICE = 'dddd0000dddd0000dddd0000dddd0000'
const OTHER_DEVICE = 'eeee0000eeee0000eeee0000eeee0000'
const SESSION_TOKEN = 'session-amara'
const FIVE_MINUTES_MS = 300_000

interface Identity {
  userId: string
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer<Identity> | null = null
let baseUrl: string

const driver = betterSqlite3()

async function start(hooks: HookConfig = {}): Promise<void> {
  sirannon = new Sirannon({ driver, hooks })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')

  server = createServer<Identity>(sirannon, {
    port: 0,
    acceptDeviceSync: true,
    authenticate: ({ headers }) => {
      if (headers.authorization !== `Bearer ${SESSION_TOKEN}`) {
        throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Unknown session')
      }
      return { userId: 'u_amara' }
    },
  })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-changes-'))
})

afterEach(async () => {
  await server?.close()
  server = null
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

function deviceChanges(deviceId = DEVICE, hlc = new HLC(deviceId).now()): ReplicationChange[] {
  return [
    {
      table: 'notes',
      operation: 'insert',
      rowId: '10',
      primaryKey: { id: 10 },
      hlc,
      txId: 'device-tx-1',
      nodeId: deviceId,
      newData: { id: 10, body: 'pushed' },
      oldData: null,
    },
  ]
}

function wireBatch(changes: ReplicationChange[]): Record<string, unknown> {
  const sourceNodeId = changes[0].nodeId
  return {
    sourceNodeId,
    batchId: `${sourceNodeId}-1-1`,
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
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${SESSION_TOKEN}` },
    body: JSON.stringify(body),
  })
}

async function errorCode(res: Response): Promise<string> {
  const body = (await res.json()) as { error: { code: string } }
  return body.error.code
}

async function storedBodies(): Promise<string[]> {
  const rows = await sirannon.get('test')?.query<{ body: string }>('SELECT body FROM notes ORDER BY id')
  return (rows ?? []).map(row => row.body)
}

describe('POST /db/:id/changes', () => {
  it('applies a valid device batch', async () => {
    await start()
    const res = await postChanges({ batch: wireBatch(deviceChanges()) })
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ applied: 1, skipped: 0, conflicts: 0 })
    expect(await storedBodies()).toEqual(['pushed'])
  })

  it('reports an already-applied batch as skipped', async () => {
    await start()
    const batch = wireBatch(deviceChanges())
    await postChanges({ batch })
    const res = await postChanges({ batch })
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ applied: 0, skipped: 1, conflicts: 0 })
  })

  it('rejects a structurally invalid batch', async () => {
    await start()
    const res = await postChanges({ batch: { sourceNodeId: 'nope' } })
    expect(res.status).toBe(400)
    expect(await errorCode(res)).toBe('INVALID_REQUEST')
  })

  it('rejects ddl operations', async () => {
    await start()
    const changes = deviceChanges()
    changes[0] = { ...changes[0], operation: 'ddl' as ReplicationChange['operation'] }
    const res = await postChanges({ batch: wireBatch(changes) })
    expect(res.status).toBe(400)
  })

  it('rejects a checksum mismatch', async () => {
    await start()
    const batch = wireBatch(deviceChanges())
    batch.checksum = 'f'.repeat(64)
    const res = await postChanges({ batch })
    expect(res.status).toBe(400)
    expect(await errorCode(res)).toBe('BATCH_VALIDATION_ERROR')
  })

  it('rejects a change naming a reserved internal table', async () => {
    await start()
    const changes = deviceChanges()
    changes[0] = {
      ...changes[0],
      table: '_sirannon_changes',
      rowId: '999999',
      primaryKey: { seq: 999999 },
      newData: {
        seq: 999999,
        table_name: 'notes',
        operation: 'delete',
        row_id: '10',
        old_data: null,
        new_data: null,
        node_id: DEVICE,
        tx_id: 'device-tx-1',
        hlc: changes[0].hlc,
      },
    }

    const res = await postChanges({ batch: wireBatch(changes) })
    expect(res.status).toBe(400)
    expect(await errorCode(res)).toBe('BATCH_VALIDATION_ERROR')
  })

  it('rejects changes whose nodeId differs from the source', async () => {
    await start()
    const changes = deviceChanges()
    changes[0] = { ...changes[0], nodeId: OTHER_DEVICE }
    const batch = wireBatch(changes)
    batch.sourceNodeId = DEVICE
    const res = await postChanges({ batch })
    expect(res.status).toBe(400)
  })
})

describe('who may push a device batch', () => {
  it('asks onBeforePush about each table the batch writes, naming the device and the caller', async () => {
    const seen: unknown[] = []
    await start({ onBeforePush: ctx => void seen.push({ ...ctx }) })

    await postChanges({ batch: wireBatch(deviceChanges()) })

    expect(seen).toEqual([{ databaseId: 'test', table: 'notes', deviceId: DEVICE, identity: { userId: 'u_amara' } }])
  })

  it('writes nothing when onBeforePush refuses the device', async () => {
    await start({
      onBeforePush: ({ deviceId }) => {
        if (deviceId !== DEVICE) throw new HookDeniedError('beforePush', 'this device belongs to another user')
      },
    })

    const res = await postChanges({ batch: wireBatch(deviceChanges(OTHER_DEVICE)) })

    expect(res.status).toBe(403)
    expect(await errorCode(res)).toBe('HOOK_DENIED')
    expect(await storedBodies()).toEqual([])
  })
})

describe('the clock on a pushed change', () => {
  it('refuses a batch stamped more than five minutes ahead of the server', async () => {
    await start()
    const ahead = HLC.encode(Date.now() + FIVE_MINUTES_MS + 60_000, 0, DEVICE)

    const res = await postChanges({ batch: wireBatch(deviceChanges(DEVICE, ahead)) })

    expect(res.status).toBe(400)
    expect(await errorCode(res)).toBe('DEVICE_CLOCK_AHEAD')
    expect(await storedBodies()).toEqual([])
  })

  it('applies a batch stamped days in the past, as a device that was offline sends it', async () => {
    await start()
    const offline = HLC.encode(Date.now() - 7 * 86_400_000, 0, DEVICE)

    const res = await postChanges({ batch: wireBatch(deviceChanges(DEVICE, offline)) })

    expect(res.status).toBe(200)
    expect(await storedBodies()).toEqual(['pushed'])
  })
})
