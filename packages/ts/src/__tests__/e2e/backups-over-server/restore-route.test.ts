import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError } from '../../../core/errors.js'
import { createServer } from '../../../server/server.js'
import { lifecycleFaultDriver } from '../../helpers/fault-drivers/lifecycle-fault-driver.js'
import { gate, integrityOf, orderLabels, storedHeads } from '../../integration/backups/harness.js'
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
  type BackedUpDatabase,
  openBackedUp,
  pause,
  replayNewestChain,
  restoreSettled,
  triggerAndSettle,
} from './served-backups.js'

const OPERATOR = { authenticate: () => ({ operator: true }) }
const COUNT = '{"sql":"SELECT label FROM orders ORDER BY id"}'
const RACING_WRITERS = 6

let dir: string

beforeEach(() => {
  dir = scratchDirectory('restore-route')
})

afterEach(releaseServedRegistries)

async function chainWithMoment(
  base: string,
  orders: BackedUpDatabase,
): Promise<{ moment: number; atMoment: string[] }> {
  await addLabels(orders.db, ['before-1', 'before-2'])
  await triggerAndSettle(base, orders, 'orders')
  await pause(20)
  const moment = Date.now()
  await pause(20)
  const atMoment = orderLabels(orders.path)
  await addLabels(orders.db, ['after-1', 'after-2'])
  await triggerAndSettle(base, orders, 'orders')
  return { moment, atMoment }
}

describe('the restore route', () => {
  it('refuses to start without an authenticate hook, and stays shut until acceptBackupRestore before it checks the body or the database', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders')
    await sirannon.open('ledger', join(dir, 'ledger.db'))
    let thrown: unknown
    try {
      createServer(sirannon, { port: 0, acceptBackupRestore: true })
    } catch (err) {
      thrown = err
    }
    expect((thrown as { code?: string }).code).toBe('INVALID_BACKUP_RESTORE')

    const shut = await serveRegistry(sirannon, {
      authenticate: ctx => {
        if (ctx.headers.authorization !== 'Bearer operator')
          throw new RequestDeniedError(401, 'NO_TOKEN', 'Present a token')
        return { operator: true }
      },
    })
    const headers = { Authorization: 'Bearer operator' }
    for (const id of ['orders', 'ledger', 'refunds']) {
      for (const body of ['{}', '[]', '{"moment":"yesterday"}', '{"batchSize":0}', 'not json', '']) {
        const answer = await sendRaw(shut.base, 'POST', `/db/${id}/backup/restore`, body, headers)
        expect([answer.status, errorCodeOf(answer.body)], `${id} ${body}`).toEqual([403, 'BACKUP_RESTORE_NOT_ACCEPTED'])
      }
    }
    const anonymous = await sendRaw(shut.base, 'POST', '/db/orders/backup/restore', '{}')
    const status = await sendRaw(shut.base, 'GET', '/db/orders/backup/restore', undefined, headers)

    expect([anonymous.status, errorCodeOf(anonymous.body)]).toEqual([401, 'NO_TOKEN'])
    expect([status.status, status.body]).toEqual([200, { state: 'idle' }])
    expect(orders.db.closed).toBe(false)
  })

  it('rebuilds the database as it stood at the moment named, answers 404 for it while rebuilding, and opens it again with its backups', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders')
    const served = await serveRegistry(sirannon, { ...OPERATOR, acceptSql: true, acceptBackupRestore: true })
    const { moment, atMoment } = await chainWithMoment(served.base, orders)
    const chainsBefore = storedHeads(orders.destination).length
    const held = gate()
    orders.destination.faults.holdRead = () => held.promise

    const accepted = await postJson(served.base, '/db/orders/backup/restore', { moment })
    await waitUntil(() => !sirannon.has('orders'), 'the database going offline')
    const running = await sendRaw(served.base, 'GET', '/db/orders/backup/restore')
    const offline = await Promise.all([
      postJson(served.base, '/db/orders/query', { sql: 'SELECT 1' }),
      postJson(served.base, '/db/orders/execute', { sql: "INSERT INTO orders (label, note) VALUES ('lost', '')" }),
      sendRaw(served.base, 'POST', '/db/orders/backup'),
      sendRaw(served.base, 'GET', '/db/orders/backup/chain'),
    ])
    const second = await postJson(served.base, '/db/orders/backup/restore', {})
    orders.destination.faults.holdRead = undefined
    held.open()
    const settled = await restoreSettled(served.base, 'orders')

    expect([accepted.status, accepted.body]).toEqual([202, { started: true }])
    expect(running.body).toMatchObject({ state: 'running', moment })
    for (const answer of offline) expect([answer.status, errorCodeOf(answer.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect([second.status, errorCodeOf(second.body)]).toEqual([409, 'BACKUP_RESTORE_IN_PROGRESS'])
    expect(settled).toMatchObject({ state: 'done', moment, report: { destPath: orders.path } })
    expect(settled.report?.restoresTo).toBeLessThanOrEqual(moment)
    expect(settled.reopenError).toBeUndefined()
    expect(integrityOf(orders.path)).toBe('ok')
    expect(orderLabels(orders.path)).toEqual(atMoment)
    expect((await sendRaw(served.base, 'POST', '/db/orders/query', COUNT)).body).toEqual({
      rows: atMoment.map(label => ({ label })),
    })

    await addLabels(sirannon.get('orders') ?? orders.db, ['after-restore'])
    const reopened = sirannon.get('orders')
    if (!reopened) throw new Error('the database did not reopen')
    await triggerAndSettle(served.base, { ...orders, db: reopened }, 'orders')
    expect(storedHeads(orders.destination).length).toBeGreaterThan(chainsBefore)
    expect(replayNewestChain(orders.destination, join(dir, 'replayed.db'))).toEqual([...atMoment, 'after-restore'])
  })

  it('starts exactly one rebuild when several restores race, and refuses every body that names no valid moment or batch size', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders')
    const served = await serveRegistry(sirannon, { ...OPERATOR, acceptBackupRestore: true })
    await addLabels(orders.db, ['kept'])
    await triggerAndSettle(served.base, orders, 'orders')

    const raced = await Promise.all(
      Array.from({ length: 6 }, () => postJson(served.base, '/db/orders/backup/restore', {})),
    )
    const statuses = raced.map(answer => answer.status).sort()
    await restoreSettled(served.base, 'orders')
    expect(statuses).toEqual([202, 409, 409, 409, 409, 409])
    for (const answer of raced.filter(each => each.status === 409)) {
      expect(errorCodeOf(answer.body)).toBe('BACKUP_RESTORE_IN_PROGRESS')
    }
    expect(orderLabels(orders.path)).toEqual(['kept'])

    const invalid: unknown[] = [
      { moment: 'yesterday' },
      { moment: -1 },
      { moment: 1.5 },
      { moment: null },
      { moment: true },
      { moment: [Date.now()] },
      { batchSize: 0 },
      { batchSize: 4097 },
      { batchSize: 1.5 },
      { batchSize: '16' },
      { batchSize: -16 },
    ]
    for (const body of invalid) {
      const answer = await postJson(served.base, '/db/orders/backup/restore', body)
      expect([answer.status, errorCodeOf(answer.body)], JSON.stringify(body)).toEqual([400, 'INVALID_REQUEST'])
    }
    const infinite = await sendRaw(served.base, 'POST', '/db/orders/backup/restore', '{"moment":1e400}')
    expect([infinite.status, errorCodeOf(infinite.body)]).toEqual([400, 'INVALID_REQUEST'])

    for (const batchSize of [1, 4096]) {
      const answer = await postJson(served.base, '/db/orders/backup/restore', { batchSize })
      const settled = await restoreSettled(served.base, 'orders')
      expect([answer.status, settled.state], `${batchSize}`).toEqual([202, 'done'])
      expect(orderLabels(orders.path), `${batchSize}`).toEqual(['kept'])
    }
    const empty = await sendRaw(served.base, 'POST', '/db/orders/backup/restore')
    expect(empty.status).toBe(202)
    expect((await restoreSettled(served.base, 'orders')).state).toBe('done')
  })

  it('reports a restore that no backup reaches or that the destination stalls, and opens the database again with the rows that it had', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders', { destinationTimeoutMs: 150 })
    const ledger = await sirannon.open('ledger', join(dir, 'ledger.db'))
    const served = await serveRegistry(sirannon, { ...OPERATOR, acceptSql: true, acceptBackupRestore: true })
    await addLabels(orders.db, ['kept-1', 'kept-2'])
    await triggerAndSettle(served.base, orders, 'orders')
    const before = orderLabels(orders.path)

    const tooEarly = await postJson(served.base, '/db/orders/backup/restore', { moment: 1_000 })
    const early = await restoreSettled(served.base, 'orders')
    orders.destination.faults.holdRead = () => new Promise<void>(() => {})
    const stalledAt = Date.now()
    const stalled = await postJson(served.base, '/db/orders/backup/restore', {})
    const stall = await restoreSettled(served.base, 'orders')
    const stallMs = Date.now() - stalledAt
    orders.destination.faults.holdRead = undefined
    const unsupported = await postJson(served.base, '/db/ledger/backup/restore', {})
    const unknown = await postJson(served.base, '/db/refunds/backup/restore', {})

    expect(tooEarly.status).toBe(202)
    expect(early).toMatchObject({ state: 'failed', moment: 1_000, error: { code: 'BACKUP_CHAIN_BROKEN' } })
    expect(stalled.status).toBe(202)
    expect(stall).toMatchObject({ state: 'failed', error: { code: 'BACKUP_DESTINATION_ERROR' } })
    expect(stall.error?.message).toMatch(/150ms/)
    expect(stallMs).toBeLessThan(5_000)
    expect([unsupported.status, errorCodeOf(unsupported.body)]).toEqual([501, 'BACKUP_UNSUPPORTED'])
    expect([unknown.status, errorCodeOf(unknown.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect(ledger.closed).toBe(false)
    expect(integrityOf(orders.path)).toBe('ok')
    expect(orderLabels(orders.path)).toEqual(before)
    expect((await sendRaw(served.base, 'POST', '/db/orders/query', COUNT)).body).toEqual({
      rows: before.map(label => ({ label })),
    })
  })

  it('leaves nothing open when the close fails, and reports a rebuild that fails to reopen as done with the reopen error beside the report', async () => {
    const faults = lifecycleFaultDriver()
    const sirannon = trackedRegistry({ driver: faults.driver })
    const stuck = await openBackedUp(sirannon, dir, 'stuck')
    const orders = await openBackedUp(sirannon, dir, 'orders')
    const served = await serveRegistry(sirannon, { ...OPERATOR, acceptSql: true, acceptBackupRestore: true })
    await addLabels(stuck.db, ['stuck-1'])
    await triggerAndSettle(served.base, stuck, 'stuck')
    faults.failClose(stuck.path)

    const closeAttempt = await postJson(served.base, '/db/stuck/backup/restore', {})
    const closeOutcome = await restoreSettled(served.base, 'stuck')
    const afterClose = await sendRaw(served.base, 'POST', '/db/stuck/query', COUNT)

    expect(closeAttempt.status).toBe(202)
    expect(closeOutcome).toMatchObject({ state: 'failed', error: { code: expect.any(String) } })
    expect(closeOutcome.report).toBeUndefined()
    expect([afterClose.status, errorCodeOf(afterClose.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect(sirannon.has('stuck')).toBe(false)
    expect(orderLabels(stuck.path)).toEqual(['stuck-1'])

    const { moment, atMoment } = await chainWithMoment(served.base, orders)
    faults.refuseOpen(orders.path)

    const accepted = await postJson(served.base, '/db/orders/backup/restore', { moment })
    const outcome = await restoreSettled(served.base, 'orders')
    const afterReopen = await sendRaw(served.base, 'POST', '/db/orders/query', COUNT)

    expect(accepted.status).toBe(202)
    expect(outcome).toMatchObject({
      state: 'done',
      moment,
      report: { destPath: orders.path },
      reopenError: { code: expect.any(String) },
    })
    expect([afterReopen.status, errorCodeOf(afterReopen.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect(integrityOf(orders.path)).toBe('ok')
    expect(orderLabels(orders.path)).toEqual(atMoment)

    faults.heal()
    await sirannon.open('orders', orders.path)
    expect((await sendRaw(served.base, 'POST', '/db/orders/query', COUNT)).body).toEqual({
      rows: atMoment.map(label => ({ label })),
    })
  })

  it('restores while clients keep writing and a backup turn is under way, keeping no refused write and losing no row from before the moment', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders')
    const served = await serveRegistry(sirannon, { ...OPERATOR, acceptSql: true, acceptBackupRestore: true })
    const { moment, atMoment } = await chainWithMoment(served.base, orders)
    const accepted = new Set<string>()
    const refused = new Map<string, string>()
    let writing = true

    const writers = Array.from({ length: RACING_WRITERS }, async (_, writer) => {
      for (let at = 0; writing; at++) {
        const label = `racer-${writer}-${at}`
        const answer = await postJson(served.base, '/db/orders/execute', {
          sql: 'INSERT INTO orders (label, note) VALUES (?, ?)',
          params: [label, ''],
        })
        if (answer.status === 200) accepted.add(label)
        else refused.set(label, `${answer.status} ${errorCodeOf(answer.body)}`)
        await pause(2)
      }
    })
    await pause(20)
    const heldTurn = gate()
    orders.destination.faults.holdWrite = () => heldTurn.promise
    const trigger = await sendRaw(served.base, 'POST', '/db/orders/backup')
    const restore = await postJson(served.base, '/db/orders/backup/restore', { moment })
    await pause(30)
    orders.destination.faults.holdWrite = undefined
    heldTurn.open()
    const settled = await restoreSettled(served.base, 'orders')
    await pause(30)
    writing = false
    await Promise.all(writers)

    const final = orderLabels(orders.path)
    expect(trigger.status).toBe(202)
    expect(restore.status).toBe(202)
    expect(settled.state).toBe('done')
    expect(integrityOf(orders.path)).toBe('ok')
    expect(final.slice(0, atMoment.length)).toEqual(atMoment)
    expect(new Set(final).size).toBe(final.length)
    for (const label of final.slice(atMoment.length)) expect(accepted.has(label), label).toBe(true)
    for (const [label, reason] of refused) {
      expect(final.includes(label), label).toBe(false)
      expect(['404 DATABASE_NOT_FOUND', '503 DATABASE_CLOSED'], label).toContain(reason)
    }
    expect(refused.size).toBeGreaterThan(0)
    expect(final.length).toBeGreaterThan(atMoment.length)
  })
})
