import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError } from '../../../core/errors.js'
import { CHAIN_LIST_NAME, gate, orderLabels, storedChain, storedHeads } from '../../integration/backups/harness.js'
import {
  errorCodeOf,
  postJson,
  releaseServedRegistries,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
  waitUntil,
} from '../lib/served-registry.js'
import {
  addLabels,
  backupStatus,
  chainsOf,
  cycleIdle,
  openBackedUp,
  pause,
  replayNewestChain,
  triggerAndSettle,
} from './served-backups.js'

const BACKUP_ROUTES: Array<[method: string, route: string, body?: string]> = [
  ['POST', '/backup', '{}'],
  ['GET', '/backup'],
  ['GET', '/backup/chain'],
  ['POST', '/backup/verify', '{"name":"anything"}'],
  ['POST', '/backup/safe-to-delete', '{}'],
  ['POST', '/backup/restore', '{}'],
  ['GET', '/backup/restore'],
]
const WRITERS = 8
const WRITES_PER_WRITER = 12

let dir: string

beforeEach(() => {
  dir = scratchDirectory('backup-routes')
})

afterEach(releaseServedRegistries)

describe('the backup routes that an operator reaches over the server', () => {
  it('answers a trigger with 202 while the turn is still copying, folds later triggers into one queued turn, and reports both through the status route', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders')
    await addLabels(orders.db, ['first'])
    const served = await serveRegistry(sirannon)
    const held = gate()
    const runsBefore = orders.runs.length
    const writesBefore = orders.destination.writes.length
    orders.destination.faults.holdWrite = () => held.promise

    const first = await sendRaw(served.base, 'POST', '/db/orders/backup')
    const duringTurn = await backupStatus(served.base, 'orders')
    const later = await Promise.all(Array.from({ length: 4 }, () => sendRaw(served.base, 'POST', '/db/orders/backup')))
    await addLabels(orders.db, ['second'])

    expect([first.status, first.body]).toEqual([202, { started: true }])
    expect(orders.destination.writes).toHaveLength(writesBefore)
    expect(duringTurn.running).toBe(true)
    for (const answer of later) expect([answer.status, answer.body]).toEqual([202, { started: true }])

    orders.destination.faults.holdWrite = undefined
    held.open()
    await waitUntil(() => orders.runs.length >= runsBefore + 2, 'the queued turn')
    await pause(100)
    const settled = await cycleIdle(served.base, 'orders')
    const [head] = storedHeads(orders.destination)

    expect(orders.runs).toHaveLength(runsBefore + 2)
    expect(orders.errors).toEqual([])
    expect(storedHeads(orders.destination)).toHaveLength(1)
    expect(settled).toMatchObject({ running: false, chainId: head?.chainId, lastRun: { chainId: head?.chainId } })
    expect(storedChain(orders.destination, head?.chainId ?? '').map(record => record.kind)).toEqual([
      'full',
      'change',
      'change',
    ])
    expect(replayNewestChain(orders.destination, join(dir, 'replayed.db'))).toEqual(['first', 'second'])
  })

  it('lists the chains that the destination stores and reads a stored backup back, refusing one that is damaged, lost, or unknown', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders', { pieceBytes: 4096 })
    await addLabels(orders.db, ['a', 'b'])
    const served = await serveRegistry(sirannon)
    await triggerAndSettle(served.base, orders, 'orders')
    await addLabels(orders.db, ['c'])
    await triggerAndSettle(served.base, orders, 'orders')

    const chains = await chainsOf(served.base, 'orders')
    const [head] = storedHeads(orders.destination)
    const records = storedChain(orders.destination, head?.chainId ?? '')
    const base = records.find(record => record.kind === 'full')
    if (!base) throw new Error('the destination holds no full copy')
    expect(chains).toHaveLength(1)
    expect(chains[0]).toMatchObject({ chainId: head?.chainId, base: { kind: 'full', name: base.name } })
    expect(chains[0]?.changes.map(change => change.name)).toEqual(
      records.filter(record => record.kind === 'change').map(record => record.name),
    )
    expect(replayNewestChain(orders.destination, join(dir, 'replayed.db'))).toEqual(orderLabels(orders.path))

    for (const record of records) {
      const indices = orders.destination.indices(record.name)
      const storedBytes = indices.reduce(
        (sum, index) => sum + orders.destination.stored(record.name, index).byteLength,
        0,
      )
      const verified = await postJson(served.base, '/db/orders/backup/verify', { name: record.name })
      expect([verified.status, verified.body], record.name).toMatchObject([
        200,
        { name: record.name, kind: record.kind, bytesRead: storedBytes, pieceCount: indices.length },
      ])
    }

    const damagedRecord = records.at(-1)
    if (!damagedRecord) throw new Error('the chain holds no record')
    const name = damagedRecord.name
    const lastIndex = orders.destination.indices(name).at(-1) ?? 0
    const original = orders.destination.stored(name, lastIndex)
    const flipped = Buffer.from(original)
    flipped[7] = (flipped[7] ?? 0) ^ 0xff
    orders.destination.replace(name, lastIndex, flipped)
    const damaged = await postJson(served.base, '/db/orders/backup/verify', { name })
    orders.destination.replace(name, lastIndex, original.subarray(0, -1))
    const short = await postJson(served.base, '/db/orders/backup/verify', { name })
    orders.destination.remove(name, lastIndex)
    const lost = await postJson(served.base, '/db/orders/backup/verify', { name })
    orders.destination.replace(name, lastIndex, original)
    const unknown = await postJson(served.base, '/db/orders/backup/verify', { name: 'nothing-stored-here' })
    const listName = await postJson(served.base, '/db/orders/backup/verify', { name: CHAIN_LIST_NAME })
    const restored = await postJson(served.base, '/db/orders/backup/verify', { name })

    expect([short.status, errorCodeOf(short.body)]).toEqual([502, 'BACKUP_DESTINATION_ERROR'])

    expect([damaged.status, errorCodeOf(damaged.body)]).toEqual([502, 'BACKUP_DESTINATION_ERROR'])
    expect([lost.status, errorCodeOf(lost.body)]).toEqual([502, 'BACKUP_DESTINATION_ERROR'])
    expect([unknown.status, errorCodeOf(unknown.body)]).toEqual([409, 'BACKUP_CHAIN_BROKEN'])
    expect([listName.status, errorCodeOf(listName.body)]).toEqual([409, 'BACKUP_CHAIN_BROKEN'])
    expect(restored.status).toBe(200)

    for (const name of [undefined, '', 5, null, ['x'], { name: 'x' }, true]) {
      const answer = await postJson(served.base, '/db/orders/backup/verify', name === undefined ? {} : { name })
      expect([answer.status, errorCodeOf(answer.body)], JSON.stringify(name)).toEqual([400, 'INVALID_REQUEST'])
    }
  })

  it('lists as safe to delete only the records that no restore inside the requested window needs', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders', { fullCopyIntervalMs: 1 })
    const served = await serveRegistry(sirannon)
    await addLabels(orders.db, ['old'])
    await triggerAndSettle(served.base, orders, 'orders')
    await pause(5)
    await addLabels(orders.db, ['new'])
    await triggerAndSettle(served.base, orders, 'orders')
    const heads = storedHeads(orders.destination)
    expect(heads).toHaveLength(3)
    const newest = heads.at(-1)?.chainId ?? ''
    const oldNames = heads
      .slice(0, -1)
      .flatMap(head => storedChain(orders.destination, head.chainId).map(record => record.name))
    const newFullCopy = storedChain(orders.destination, newest).find(record => record.kind === 'full')
    expect(replayNewestChain(orders.destination, join(dir, 'replayed.db'))).toEqual(['old', 'new'])

    const unwindowed = await postJson(served.base, '/db/orders/backup/safe-to-delete', {})
    const emptyBody = await sendRaw(served.base, 'POST', '/db/orders/backup/safe-to-delete')
    const everything = await postJson(served.base, '/db/orders/backup/safe-to-delete', { restorableFrom: 0 })
    const recent = await postJson(served.base, '/db/orders/backup/safe-to-delete', {
      restorableFrom: newFullCopy?.finishedAt,
    })

    expect([unwindowed.status, unwindowed.body]).toEqual([200, { records: [] }])
    expect([emptyBody.status, emptyBody.body]).toEqual([200, { records: [] }])
    expect([everything.status, everything.body]).toEqual([200, { records: [] }])
    expect(recent.status).toBe(200)
    expect((recent.body as { records: Array<{ name: string }> }).records.map(record => record.name).sort()).toEqual(
      [...oldNames].sort(),
    )

    for (const restorableFrom of ['yesterday', -1, 1.5, null, true, [0], { at: 0 }]) {
      const answer = await postJson(served.base, '/db/orders/backup/safe-to-delete', { restorableFrom })
      expect([answer.status, errorCodeOf(answer.body)], JSON.stringify(restorableFrom)).toEqual([
        400,
        'INVALID_REQUEST',
      ])
    }
    const infinite = await sendRaw(served.base, 'POST', '/db/orders/backup/safe-to-delete', '{"restorableFrom":1e400}')
    expect([infinite.status, errorCodeOf(infinite.body)]).toEqual([400, 'INVALID_REQUEST'])
  })

  it('refuses malformed bodies, unknown or closed databases, and databases opened without backups on every backup route', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders')
    await sirannon.open('ledger', join(dir, 'ledger.db'))
    await sirannon.open('closing', join(dir, 'closing.db'), { backups: { destination: orders.destination } })
    const served = await serveRegistry(sirannon, { acceptBackupRestore: true, authenticate: () => undefined })
    await sirannon.close('closing')

    for (const [method, route, body] of BACKUP_ROUTES) {
      const unknown = await sendRaw(served.base, method, `/db/refunds${route}`, body)
      const closed = await sendRaw(served.base, method, `/db/closing${route}`, body)
      const unsupported = await sendRaw(served.base, method, `/db/ledger${route}`, body)
      const label = `${method} ${route}`
      if (method === 'GET' && route === '/backup/restore') {
        expect([unknown.status, unknown.body], label).toEqual([200, { state: 'idle' }])
        expect([unsupported.status, unsupported.body], label).toEqual([200, { state: 'idle' }])
        continue
      }
      expect([unknown.status, errorCodeOf(unknown.body)], label).toEqual([404, 'DATABASE_NOT_FOUND'])
      expect([closed.status, errorCodeOf(closed.body)], label).toEqual([404, 'DATABASE_NOT_FOUND'])
      expect([unsupported.status, errorCodeOf(unsupported.body)], label).toEqual([501, 'BACKUP_UNSUPPORTED'])
    }

    const bodies: Array<[body: string, code: string]> = [
      ['0', 'INVALID_REQUEST'],
      ['[]', 'INVALID_REQUEST'],
      ['[{"name":"x"}]', 'INVALID_REQUEST'],
      ['"x"', 'INVALID_REQUEST'],
      ['null', 'INVALID_REQUEST'],
      ['true', 'INVALID_REQUEST'],
      ['{"name":', 'INVALID_JSON'],
      ['name=x', 'INVALID_JSON'],
    ]
    for (const route of ['/backup', '/backup/verify', '/backup/safe-to-delete', '/backup/restore']) {
      for (const [body, code] of bodies) {
        const answer = await sendRaw(served.base, 'POST', `/db/orders${route}`, body)
        expect([answer.status, errorCodeOf(answer.body)], `${route} ${body}`).toEqual([400, code])
      }
    }
    expect(orders.runs).toHaveLength(1)
    expect((await backupStatus(served.base, 'orders')).running).toBe(false)
  })

  it('calls the authenticate hook before every backup route, so one hook can keep them for an operator', async () => {
    const sirannon = trackedRegistry()
    await openBackedUp(sirannon, dir, 'orders')
    const seen: string[] = []
    const served = await serveRegistry(sirannon, {
      acceptSql: true,
      acceptBackupRestore: true,
      authenticate: ctx => {
        seen.push(`${ctx.method} ${ctx.path}`)
        const operator = ctx.headers.authorization === 'Bearer operator'
        if (ctx.path.startsWith(`/db/${ctx.databaseId}/backup`) && !operator) {
          throw new RequestDeniedError(403, 'HOOK_DENIED', 'Only an operator may reach the backups')
        }
        return { operator }
      },
    })

    for (const [method, route, body] of BACKUP_ROUTES) {
      const refused = await sendRaw(served.base, method, `/db/orders${route}`, body, { Authorization: 'Bearer app' })
      expect([refused.status, refused.body], `${method} ${route}`).toEqual([
        403,
        { error: { code: 'HOOK_DENIED', message: 'Only an operator may reach the backups' } },
      ])
    }
    const data = await sendRaw(served.base, 'POST', '/db/orders/query', '{"sql":"SELECT count(*) AS n FROM orders"}', {
      Authorization: 'Bearer app',
    })
    const operatorStatus = await sendRaw(served.base, 'GET', '/db/orders/backup', undefined, {
      Authorization: 'Bearer operator',
    })

    expect([data.status, data.body]).toEqual([200, { rows: [{ n: 0 }] }])
    expect(operatorStatus.status).toBe(200)
    expect(seen.slice(0, BACKUP_ROUTES.length)).toEqual(
      BACKUP_ROUTES.map(([method, route]) => `${method.toLowerCase()} /db/orders${route}`),
    )
  })

  it('reports a turn that the destination refuses or stalls through the status route, then backs up once the destination recovers', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders', { destinationTimeoutMs: 150 })
    await addLabels(orders.db, ['kept'])
    const served = await serveRegistry(sirannon)

    orders.destination.faults.refuseWrite = () => true
    const refused = await sendRaw(served.base, 'POST', '/db/orders/backup')
    await waitUntil(() => orders.errors.length === 1, 'the refused turn')
    const afterRefusal = await cycleIdle(served.base, 'orders')
    orders.destination.faults.refuseWrite = undefined

    orders.destination.faults.holdWrite = () => new Promise<void>(() => {})
    const stalledAt = Date.now()
    const stalled = await sendRaw(served.base, 'POST', '/db/orders/backup')
    await waitUntil(() => orders.errors.length === 2, 'the stalled turn')
    const stallMs = Date.now() - stalledAt
    const afterStall = await cycleIdle(served.base, 'orders')
    orders.destination.faults.holdWrite = undefined

    expect(refused.status).toBe(202)
    expect(stalled.status).toBe(202)
    expect(afterRefusal).toMatchObject({ running: false, lastError: { code: 'BACKUP_DESTINATION_ERROR' } })
    expect(afterRefusal.lastError?.chainId).toBe(storedHeads(orders.destination)[0]?.chainId)
    expect(afterStall).toMatchObject({ lastError: { code: 'BACKUP_DESTINATION_ERROR' } })
    expect(afterStall.lastError?.message).toMatch(/150ms/)
    expect(stallMs).toBeLessThan(3_000)
    expect(afterRefusal.lastRun?.runId).toBe(orders.runs[0]?.runId)
    expect(orders.runs).toHaveLength(1)

    await triggerAndSettle(served.base, orders, 'orders')
    expect(replayNewestChain(orders.destination, join(dir, 'replayed.db'))).toEqual(['kept'])
  })

  it('keeps every write that clients send during backup turns, so the newest chain rebuilds the file row for row', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders', { pieceBytes: 4096 })
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const expected: string[] = []
    let writing = true

    const triggers = (async () => {
      const answers: number[] = []
      while (writing) {
        answers.push((await sendRaw(served.base, 'POST', '/db/orders/backup')).status)
        await pause(3)
      }
      return answers
    })()
    await Promise.all(
      Array.from({ length: WRITERS }, async (_, writer) => {
        for (let at = 0; at < WRITES_PER_WRITER; at++) {
          const label = `w${writer}-${at}`
          expected.push(label)
          const answer = await postJson(served.base, '/db/orders/execute', {
            sql: 'INSERT INTO orders (label, note) VALUES (?, ?)',
            params: [label, 'x'.repeat(300)],
          })
          expect(answer.status, answer.text).toBe(200)
        }
      }),
    )
    writing = false
    const statuses = await triggers
    await triggerAndSettle(served.base, orders, 'orders')

    expect(statuses.every(status => status === 202)).toBe(true)
    expect(orders.runs.length).toBeGreaterThan(2)
    expect(orders.errors).toEqual([])
    expect([...orderLabels(orders.path)].sort()).toEqual([...expected].sort())
    expect(replayNewestChain(orders.destination, join(dir, 'replayed.db'))).toEqual(orderLabels(orders.path))
  })
})
