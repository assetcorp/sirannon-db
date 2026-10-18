import { afterEach, describe, expect, it } from 'vitest'
import { HookDeniedError, RequestDeniedError } from '../../../core/errors.js'
import type { BeforeSubscribeHook, OperationRegistry } from '../../../core/types.js'
import { postJson } from '../lib/served-registry.js'
import {
  pause,
  QUIET_PERIOD_MS,
  releaseChangeStreams,
  type ServedDatabase,
  serveDatabase,
  triggerNames,
  WireSocket,
} from './harness.js'

const DEVICE = 'abcd0000abcd0000abcd0000abcd0000'
const HOOK_DELAY_MS = 120
const SCHEMA = [
  'CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT, tag BLOB)',
  'CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)',
  'CREATE TABLE audit (id INTEGER PRIMARY KEY, entry TEXT)',
  'CREATE TABLE secrets (id INTEGER PRIMARY KEY, value TEXT)',
]

const operations: OperationRegistry = {
  vault: { reads: { everyNote: { statement: () => ({ sql: 'SELECT id, title FROM notes ORDER BY id' }) } } },
}

afterEach(releaseChangeStreams)

function vault(onBeforeSubscribe: BeforeSubscribeHook): Promise<ServedDatabase> {
  return serveDatabase({
    label: 'subscribe-hooks',
    databaseId: 'vault',
    schema: SCHEMA,
    registry: { hooks: { onBeforeSubscribe } },
    server: {
      operations,
      acceptDeviceSync: true,
      authenticate: ctx => (ctx.headers.authorization === 'Bearer acme' ? { tenant: 'acme' } : undefined),
    },
  })
}

describe('the subscribe hook on served subscriptions', () => {
  it('receives the database, the table, the decoded filter, the identity, and the device, once per table, and is awaited', async () => {
    const seen: unknown[] = []
    const served = await vault(async ctx => {
      seen.push({ ...ctx, filter: ctx.filter === undefined ? undefined : { ...ctx.filter } })
      await pause(HOOK_DELAY_MS)
    })
    const ws = await WireSocket.open(`${served.served.wsBase}/db/vault`, { authorization: 'Bearer acme' })
    const anonymous = await WireSocket.open(`${served.served.wsBase}/db/vault`)

    const sentAt = Date.now()
    const filtered = await ws.request({
      id: 'filtered',
      type: 'subscribe',
      table: 'notes',
      filter: { id: { __sirannon_int: '9007199254740993' }, tag: { __sirannon_blob: '00FF' }, title: 'plans' },
    })
    const waited = Date.now() - sentAt
    const device = await ws.request({ id: 'device', type: 'subscribe', tables: ['notes', 'ledger'], deviceId: DEVICE })
    const live = await ws.request({ id: 'live', type: 'subscribe', name: 'everyNote' })
    const unidentified = await anonymous.request({ id: 'plain', type: 'subscribe', table: 'audit' })

    expect([filtered.type, device.type, live.type, unidentified.type]).toEqual([
      'subscribed',
      'subscribed',
      'subscribed',
      'subscribed',
    ])
    expect(waited).toBeGreaterThanOrEqual(HOOK_DELAY_MS)
    expect(seen).toEqual([
      {
        databaseId: 'vault',
        table: 'notes',
        filter: { id: 9007199254740993n, tag: Buffer.from([0x00, 0xff]), title: 'plans' },
        identity: { tenant: 'acme' },
      },
      { databaseId: 'vault', table: 'notes', identity: { tenant: 'acme' }, deviceId: DEVICE },
      { databaseId: 'vault', table: 'ledger', identity: { tenant: 'acme' }, deviceId: DEVICE },
      { databaseId: 'vault', table: 'audit' },
    ])
  })

  it('refuses each subscription the hook throws for with the code it carries, and delivers nothing from those tables', async () => {
    const served = await vault(ctx => {
      if (ctx.table === 'ledger') throw new HookDeniedError('onBeforeSubscribe', 'the ledger is closed')
      if (ctx.table === 'secrets') throw new RequestDeniedError(403, 'SECRETS_SEALED', 'The secrets stay sealed')
      if (ctx.table === 'audit') throw new Error('the auditor said no')
    })
    const ws = await WireSocket.open(`${served.served.wsBase}/db/vault`, { authorization: 'Bearer acme' })

    const refusals = [
      await ws.request({ id: 'ledger', type: 'subscribe', table: 'ledger' }),
      await ws.request({ id: 'secrets', type: 'subscribe', table: 'secrets', sinceSeq: '0' }),
      await ws.request({ id: 'audit', type: 'subscribe', table: 'audit' }),
      await ws.request({ id: 'device', type: 'subscribe', tables: ['notes', 'ledger'], deviceId: DEVICE }),
    ]
    const admitted = await ws.request({ id: 'notes', type: 'subscribe', table: 'notes' })
    for (const sql of [
      "INSERT INTO notes (title) VALUES ('visible')",
      'INSERT INTO ledger (amount) VALUES (500)',
      "INSERT INTO audit (entry) VALUES ('hidden')",
      "INSERT INTO secrets (value) VALUES ('sealed')",
    ]) {
      const answer = await postJson(served.served.base, '/db/vault/execute', { sql })
      expect(answer.status, answer.text).toBe(200)
    }
    await ws.waitForEvents('notes', 1)
    await pause(QUIET_PERIOD_MS)

    expect(refusals.map(reply => [reply.type, reply.error?.code])).toEqual([
      ['error', 'HOOK_DENIED'],
      ['error', 'SECRETS_SEALED'],
      ['error', 'INTERNAL_ERROR'],
      ['error', 'HOOK_DENIED'],
    ])
    expect(refusals[2]?.error?.message).not.toContain('auditor')
    expect(admitted.type).toBe('subscribed')
    expect(ws.messages.filter(message => message.type === 'change').map(message => message.id)).toEqual(['notes'])
    expect(triggerNames(served.path)).toEqual([
      '_sirannon_trg_notes_delete',
      '_sirannon_trg_notes_insert',
      '_sirannon_trg_notes_insert_conflicts',
      '_sirannon_trg_notes_update',
      '_sirannon_trg_notes_update_conflicts',
    ])
  })
})
