import { createHash } from 'node:crypto'
import { afterEach, describe, expect, it } from 'vitest'
import { HookDeniedError } from '../../../core/errors.js'
import type { PushHookContext } from '../../../core/hooks/types.js'
import { decodeHlc, encodeHlc, oracleDisagreementsWithSpecVectors, specChecksum } from '../lib/device-sync-oracle.js'
import { errorCodeOf, readSqlite, releaseServedRegistries, sendRaw, within } from '../lib/served-registry.js'
import {
  DEVICE,
  deviceChange,
  OTHER_DEVICE,
  push,
  pushBody,
  SESSION_TOKEN,
  serverChangeLog,
  serverMeta,
  startDeviceServer,
  storedNotes,
  wireBatch,
} from './harness.js'

const FIVE_MINUTES_MS = 300_000
const WEEK_MS = 7 * 86_400_000
const BEYOND_SAFE = 2n ** 63n - 1n

afterEach(releaseServedRegistries)

describe('pushing a device batch', () => {
  it('applies a batch, keeps every value intact, and stamps each applied change with the device as its origin', async () => {
    const server = await startDeviceServer()
    await server.db.execute(
      'CREATE TABLE files (id INTEGER PRIMARY KEY, label TEXT, payload BLOB, size INTEGER, ratio REAL)',
    )
    await server.db.watch('files')
    const payload = new Uint8Array([0, 255, 1, 254])
    const earlier = encodeHlc(Date.now() - 1_000, 0, DEVICE)
    const later = encodeHlc(Date.now() - 500, 3, DEVICE)
    const changes = [
      deviceChange({
        table: 'files',
        id: 1,
        txId: 'device-tx-a',
        hlc: earlier,
        newData: { id: 1, label: 'héllo 東京 🚀', payload, size: BEYOND_SAFE, ratio: 0.25 },
      }),
      deviceChange({
        table: 'files',
        operation: 'update',
        id: 1,
        txId: 'device-tx-a',
        hlc: later,
        newData: { id: 1, label: 'second', payload, size: -BEYOND_SAFE, ratio: -1.5 },
        oldData: { id: 1, label: 'héllo 東京 🚀', payload, size: BEYOND_SAFE, ratio: 0.25 },
      }),
      deviceChange({ id: 7, txId: 'device-tx-b', hlc: later }),
    ]

    const answer = await push(server.base, wireBatch(changes))

    expect([answer.status, answer.body]).toEqual([200, { applied: 3, skipped: 0, conflicts: 0 }])
    expect(readSqlite(server.path, 'SELECT id, label, payload, size, ratio FROM files')).toEqual([
      { id: 1n, label: 'second', payload: Buffer.from(payload), size: -BEYOND_SAFE, ratio: -1.5 },
    ])
    expect(storedNotes(server.path)).toEqual([{ id: 7, body: 'pushed 7' }])
    expect(serverChangeLog(server.path).map(row => [row.row_id, row.node_id, row.tx_id, row.hlc])).toEqual([
      ['1', DEVICE, 'device-tx-a', later],
      ['1', DEVICE, 'device-tx-a', later],
      ['7', DEVICE, 'device-tx-b', later],
    ])
  })

  it('accepts only the checksum that the spec canonical form produces, whatever order the fields arrive in', async () => {
    expect(oracleDisagreementsWithSpecVectors()).toEqual([])
    const server = await startDeviceServer()
    const bodies = [
      '',
      'a'.repeat(23),
      'b'.repeat(24),
      'c'.repeat(31),
      'ü'.repeat(40),
      '東京'.repeat(300),
      '"\\\n\u0001',
    ]
    const changes = bodies.map((body, at) => deviceChange({ id: at + 1, newData: { body, id: at + 1 } }))
    const batch = wireBatch(changes)
    const right = specChecksum(changes)
    const wrong = [
      right.toUpperCase(),
      createHash('sha256').update(JSON.stringify(changes)).digest('hex'),
      createHash('sha256').update(JSON.stringify(batch.changes)).digest('hex'),
      specChecksum(changes.slice(1)),
      'f'.repeat(64),
      `${right} `,
    ]

    for (const checksum of wrong) {
      const answer = await push(server.base, { ...batch, checksum })
      expect([answer.status, errorCodeOf(answer.body)], checksum).toEqual([400, 'BATCH_VALIDATION_ERROR'])
    }
    expect(storedNotes(server.path)).toEqual([])

    const reordered = {
      ...batch,
      changes: (batch.changes as Array<Record<string, unknown>>).map(change =>
        Object.fromEntries(Object.entries(change).reverse()),
      ),
    }
    const answer = await push(server.base, reordered)
    expect([answer.status, answer.body]).toEqual([200, { applied: bodies.length, skipped: 0, conflicts: 0 }])
    expect(storedNotes(server.path).map(note => note.body)).toEqual(bodies)
  })

  it('skips a batch that it already applied, and reports each skipped change', async () => {
    const server = await startDeviceServer()
    const batch = wireBatch([deviceChange({ id: 1 }), deviceChange({ id: 2 })], { fromSeq: '5' })
    await push(server.base, batch)
    await server.db.execute("UPDATE notes SET body = 'server edit' WHERE id = 1")
    const logged = serverChangeLog(server.path).length

    const replay = await push(server.base, batch)
    const earlier = await push(server.base, wireBatch([deviceChange({ id: 1 })], { fromSeq: '4', toSeq: '6' }))

    expect([replay.status, replay.body]).toEqual([200, { applied: 0, skipped: 2, conflicts: 0 }])
    expect([earlier.status, earlier.body]).toEqual([200, { applied: 0, skipped: 1, conflicts: 0 }])
    expect(storedNotes(server.path)).toEqual([
      { id: 1, body: 'server edit' },
      { id: 2, body: 'pushed 2' },
    ])
    expect(serverChangeLog(server.path)).toHaveLength(logged)
  })

  it('skips the applied changes of a retried batch that overlaps them, so a row that the server deleted stays deleted', async () => {
    const server = await startDeviceServer()
    const first = deviceChange({ id: 10, hlc: encodeHlc(Date.now() - 10_000, 0, DEVICE) })
    await push(server.base, wireBatch([first], { fromSeq: '1', toSeq: '1' }))
    await server.db.execute('DELETE FROM notes WHERE id = 10')

    const retried = await push(server.base, wireBatch([first, deviceChange({ id: 11 })], { fromSeq: '1', toSeq: '2' }))

    expect([retried.status, retried.body]).toEqual([200, { applied: 1, skipped: 1, conflicts: 0 }])
    expect(storedNotes(server.path)).toEqual([{ id: 11, body: 'pushed 11' }])
  })

  it('refuses a change that names an internal table or carries a schema change, and writes nothing', async () => {
    const server = await startDeviceServer()
    const logged = serverChangeLog(server.path)
    const forged: Array<[ReturnType<typeof deviceChange>, number, string]> = [
      [
        deviceChange({
          table: '_sirannon_changes',
          id: 999_999,
          newData: { seq: 999_999, table_name: 'notes', operation: 'DELETE', row_id: '1', node_id: DEVICE },
        }),
        400,
        'BATCH_VALIDATION_ERROR',
      ],
      [
        deviceChange({ table: '_sirannon_meta', id: 1, newData: { key: 'node_id', value: DEVICE } }),
        400,
        'BATCH_VALIDATION_ERROR',
      ],
      [deviceChange({ table: 'sqlite_master', id: 1 }), 400, 'BATCH_VALIDATION_ERROR'],
      [{ ...deviceChange({ id: 1 }), operation: 'ddl' }, 400, 'INVALID_REQUEST'],
    ]

    for (const [change, status, code] of forged) {
      const answer = await push(server.base, wireBatch([deviceChange({ id: 2 }), change]))
      expect([answer.status, errorCodeOf(answer.body)], change.table).toEqual([status, code])
    }

    expect(storedNotes(server.path)).toEqual([])
    expect(serverChangeLog(server.path)).toEqual(logged)
    expect(serverMeta(server.path, 'node_id')).not.toBe(DEVICE)
  })

  it('refuses a change for a table that the server lacks with a client error, and writes nothing', async () => {
    const server = await startDeviceServer()
    const changes = [
      deviceChange({ id: 2, txId: 'tx-shared' }),
      deviceChange({ table: 'ghost', id: 3, txId: 'tx-shared' }),
    ]

    const answer = await push(server.base, wireBatch(changes))

    expect(answer.status, JSON.stringify(answer.body)).toBeGreaterThanOrEqual(400)
    expect(answer.status).toBeLessThan(500)
    expect(storedNotes(server.path)).toEqual([])
  })

  it('refuses every malformed body with 400 and the code that names the fault, and keeps serving', async () => {
    const server = await startDeviceServer({ server: { maxBodyBytes: 600_000 } })
    const good = wireBatch([deviceChange({ id: 1 })])
    const goodChange = (good.changes as Array<Record<string, unknown>>)[0]
    const withChange = (patch: Record<string, unknown>) => ({
      batch: { ...good, changes: [{ ...goodChange, ...patch }] },
    })
    const thousandAndOne = Array.from({ length: 1_001 }, (_, at) => deviceChange({ id: at + 1 }))
    const malformed: Array<[string, unknown]> = [
      ['no batch', {}],
      ['a null batch', { batch: null }],
      ['a list batch', { batch: [good] }],
      ['an upper-case device id', { batch: { ...good, sourceNodeId: DEVICE.toUpperCase() } }],
      ['a 31-character device id', { batch: { ...good, sourceNodeId: DEVICE.slice(1) } }],
      ['an empty batch id', { batch: { ...good, batchId: '' } }],
      ['a 129-character batch id', { batch: { ...good, batchId: 'b'.repeat(129) } }],
      ['a negative sequence', { batch: { ...good, fromSeq: '-1' } }],
      ['a fractional sequence', { batch: { ...good, fromSeq: '1.5' } }],
      ['a 20-digit sequence', { batch: { ...good, toSeq: '1'.repeat(20) } }],
      ['a numeric sequence', { batch: { ...good, fromSeq: 1 } }],
      ['a range that runs backwards', { batch: { ...good, fromSeq: '9', toSeq: '3' } }],
      ['no clock range', { batch: { ...good, hlcRange: undefined } }],
      ['a numeric clock range', { batch: { ...good, hlcRange: { min: 1, max: 2 } } }],
      ['no changes', { batch: { ...good, changes: [] } }],
      ['1001 changes', { batch: wireBatch(thousandAndOne) }],
      ['an empty checksum', { batch: { ...good, checksum: '' } }],
      ['a table name with a space', withChange({ table: 'bad name' })],
      ['a table name opening with a digit', withChange({ table: '1notes' })],
      ['an unknown operation', withChange({ operation: 'upsert' })],
      ['an upper-case operation', withChange({ operation: 'INSERT' })],
      ['an empty row id', withChange({ rowId: '' })],
      ['a null primary key', withChange({ primaryKey: null })],
      ['an empty clock', withChange({ hlc: '' })],
      ['a clock with no wall time', withChange({ hlc: 'not-a-clock' })],
      ['an empty transaction id', withChange({ txId: '' })],
      ['a change from another device', withChange({ nodeId: OTHER_DEVICE })],
      ['a list as the new row', withChange({ newData: [1] })],
      ['a string as the old row', withChange({ oldData: 'x' })],
      ['a malformed integer envelope', withChange({ newData: { id: { __sirannon_int: 'twelve' }, body: 'x' } })],
      ['a malformed blob envelope', withChange({ newData: { id: 1, body: { __sirannon_blob: 'XYZ' } } })],
      ['a negative schema version', { schemaVersion: -1, batch: good }],
      ['a fractional schema version', { schemaVersion: 1.5, batch: good }],
      ['a string schema version', { schemaVersion: '0', batch: good }],
    ]

    for (const [label, body] of malformed) {
      const answer = await pushBody(server.base, body)
      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([400, 'INVALID_REQUEST'])
    }
    for (const [label, text, code] of [
      ['truncated JSON', '{"batch":{', 'INVALID_JSON'],
      ['plain text', 'batch', 'INVALID_JSON'],
      ['an empty body', '', 'EMPTY_BODY'],
    ]) {
      const answer = await sendRaw(server.base, 'POST', '/db/app/changes', text, {
        authorization: `Bearer ${SESSION_TOKEN}`,
      })
      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([400, code])
    }
    const oversized = await push(server.base, { ...good, batchId: 'x'.repeat(700_000) })
    expect([oversized.status, errorCodeOf(oversized.body)]).toEqual([413, 'PAYLOAD_TOO_LARGE'])

    expect(storedNotes(server.path)).toEqual([])
    const largest = await push(server.base, wireBatch(thousandAndOne.slice(0, 1_000), { batchId: 'b'.repeat(128) }))
    expect([largest.status, largest.body]).toEqual([200, { applied: 1_000, skipped: 0, conflicts: 0 }])
  })

  it('asks onBeforePush once for each table, naming the device and the caller, and writes nothing when it refuses', async () => {
    const seen: PushHookContext[] = []
    const server = await startDeviceServer({
      hooks: {
        onBeforePush: async context => {
          seen.push({ ...context })
          await new Promise(resolve => setTimeout(resolve, 20))
          if (context.deviceId !== DEVICE)
            throw new HookDeniedError('beforePush', 'this device belongs to another user')
        },
      },
    })
    await server.db.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, body TEXT)')

    const allowed = await push(
      server.base,
      wireBatch([deviceChange({ id: 1 }), deviceChange({ id: 2 }), deviceChange({ table: 'tags', id: 3 })]),
    )
    const refused = await push(server.base, wireBatch([deviceChange({ id: 4, nodeId: OTHER_DEVICE })]))

    expect(allowed.status).toBe(200)
    expect([refused.status, errorCodeOf(refused.body)]).toEqual([403, 'HOOK_DENIED'])
    expect(seen).toEqual([
      { databaseId: 'app', table: 'notes', deviceId: DEVICE, identity: { userId: 'u_amara' } },
      { databaseId: 'app', table: 'tags', deviceId: DEVICE, identity: { userId: 'u_amara' } },
      { databaseId: 'app', table: 'notes', deviceId: OTHER_DEVICE, identity: { userId: 'u_amara' } },
    ])
    expect(storedNotes(server.path).map(note => note.id)).toEqual([1, 2])
  })

  it('refuses a batch stamped more than five minutes ahead of the server, and accepts one stamped days or decades behind', async () => {
    const server = await startDeviceServer()
    const aheadBy = (ms: number, id: number) => deviceChange({ id, hlc: encodeHlc(Date.now() + ms, 0, DEVICE) })

    const tooFar = await push(server.base, wireBatch([aheadBy(0, 1), aheadBy(FIVE_MINUTES_MS + 5_000, 2)]))
    expect([tooFar.status, errorCodeOf(tooFar.body)]).toEqual([400, 'DEVICE_CLOCK_AHEAD'])
    expect(storedNotes(server.path)).toEqual([])

    const behind = [
      deviceChange({ id: 3, hlc: encodeHlc(Date.now() - WEEK_MS, 0, DEVICE) }),
      deviceChange({ id: 4, hlc: encodeHlc(0, 0, DEVICE) }),
      aheadBy(FIVE_MINUTES_MS - 5_000, 5),
    ]
    const accepted = await push(server.base, wireBatch(behind, { fromSeq: '10' }))
    expect([accepted.status, accepted.body]).toEqual([200, { applied: 3, skipped: 0, conflicts: 0 }])
    expect(storedNotes(server.path).map(note => note.id)).toEqual([3, 4, 5])
  })

  it('keeps the server clock within five minutes of real time whatever clock range a batch declares', async () => {
    const server = await startDeviceServer()
    const now = Date.now()
    const farAhead = encodeHlc(now + 10 * 365 * 86_400_000, 0, DEVICE)
    const batch = wireBatch([deviceChange({ id: 1, hlc: encodeHlc(now, 0, DEVICE) })], {
      hlcRange: { min: farAhead, max: farAhead },
    })

    const answer = await push(server.base, batch)
    await server.db.execute("INSERT INTO notes (id, body) VALUES (2, 'server write after the push')")

    expect(answer.status).toBeGreaterThanOrEqual(200)
    const serverStamp = serverChangeLog(server.path).find(row => row.row_id === '2')?.hlc ?? ''
    expect(decodeHlc(serverStamp)?.wallMs).toBeLessThanOrEqual(Date.now() + FIVE_MINUTES_MS)
    expect(decodeHlc(serverMeta(server.path, 'hlc_clock') ?? '')?.wallMs).toBeLessThanOrEqual(
      Date.now() + FIVE_MINUTES_MS,
    )
  })

  it('settles a device change against the server row by last writer wins, deletes included, and lets a later edit bring back an earlier deleted row', async () => {
    const server = await startDeviceServer()
    await server.db.executeBatch('INSERT INTO notes (id, body) VALUES (?, ?)', [
      [1, 'server and newer'],
      [2, 'server and newer'],
      [3, 'server'],
    ])
    await server.db.execute('DELETE FROM notes WHERE id = 3')
    const weekOld = encodeHlc(Date.now() - WEEK_MS, 0, DEVICE)
    const fresh = encodeHlc(Date.now() + 1_000, 0, DEVICE)
    const changes = [
      deviceChange({ operation: 'update', id: 1, hlc: weekOld, newData: { id: 1, body: 'older device edit' } }),
      deviceChange({ operation: 'delete', id: 2, hlc: weekOld, oldData: { id: 2, body: 'server and newer' } }),
      deviceChange({ operation: 'update', id: 3, hlc: fresh, newData: { id: 3, body: 'edit of a deleted row' } }),
    ]

    const first = await push(server.base, wireBatch(changes))
    const second = await push(
      server.base,
      wireBatch(
        [
          deviceChange({ operation: 'update', id: 1, hlc: fresh, newData: { id: 1, body: 'newer device edit' } }),
          deviceChange({ operation: 'delete', id: 2, hlc: fresh, oldData: { id: 2, body: 'server and newer' } }),
        ],
        { fromSeq: '4' },
      ),
    )
    const stale = await push(
      server.base,
      wireBatch([deviceChange({ id: 2, hlc: weekOld, newData: { id: 2, body: 'insert older than the delete' } })], {
        fromSeq: '6',
      }),
    )

    expect([first.status, first.body]).toEqual([200, { applied: 1, skipped: 2, conflicts: 2 }])
    expect([second.status, second.body]).toEqual([200, { applied: 2, skipped: 0, conflicts: 2 }])
    expect([stale.status, stale.body]).toEqual([200, { applied: 0, skipped: 1, conflicts: 0 }])
    expect(storedNotes(server.path)).toEqual([
      { id: 1, body: 'newer device edit' },
      { id: 3, body: 'edit of a deleted row' },
    ])
  })

  it('refuses a device whose schema version differs from the server with the version the server holds', async () => {
    const server = await startDeviceServer()
    await server.db.migrate([{ version: 1, name: 'tags', up: 'CREATE TABLE tags (id INTEGER PRIMARY KEY)' }])
    const batch = wireBatch([deviceChange({ id: 1 })])

    const outcomes = []
    for (const schemaVersion of [undefined, 0, 2, 1]) {
      const answer = await push(server.base, batch, schemaVersion)
      outcomes.push([
        answer.status,
        errorCodeOf(answer.body),
        (answer.body as { error?: { details?: unknown } }).error?.details,
      ])
    }

    expect(outcomes).toEqual([
      [409, 'MIGRATION_REQUIRED', { serverVersion: 1 }],
      [409, 'MIGRATION_REQUIRED', { serverVersion: 1 }],
      [409, 'SCHEMA_AHEAD', { serverVersion: 1 }],
      [200, null, undefined],
    ])
    expect(storedNotes(server.path).map(note => note.id)).toEqual([1])
  })

  it('holds a transaction that continues across batches until its last change arrives, and applies it whole', async () => {
    const server = await startDeviceServer()
    const sequenced = (changes: ReturnType<typeof deviceChange>[], firstSeq: number, txContinues: boolean) => {
      const batch = wireBatch(changes, { fromSeq: String(firstSeq) })
      const wired = (batch.changes as Array<Record<string, unknown>>).map((change, at) => ({
        ...change,
        seq: String(firstSeq + at),
      }))
      return { ...batch, changes: wired, ...(txContinues ? { txContinues: true } : {}) }
    }
    const big = (id: number) => deviceChange({ id, txId: 'tx-big' })
    const partOne = sequenced([big(1), big(2), big(3)], 1, true)
    const partTwo = sequenced([big(4), big(5)], 4, true)
    const lastPart = sequenced([big(6), deviceChange({ id: 7, txId: 'tx-after' })], 6, false)
    const counts: number[] = []

    for (const [part, skipped] of [
      [partOne, 0],
      [partOne, 3],
      [partTwo, 0],
      [partTwo, 2],
    ] as const) {
      const answer = await push(server.base, part)
      expect([answer.status, answer.body]).toEqual([200, { applied: 0, skipped, conflicts: 0 }])
      counts.push(storedNotes(server.path).length)
    }
    const completed = await push(server.base, lastPart)
    const replayed = await push(server.base, lastPart)

    expect(counts).toEqual([0, 0, 0, 0])
    expect([completed.status, completed.body]).toEqual([200, { applied: 7, skipped: 0, conflicts: 0 }])
    expect([replayed.status, replayed.body]).toEqual([200, { applied: 0, skipped: 2, conflicts: 0 }])
    expect(storedNotes(server.path).map(note => note.id)).toEqual([1, 2, 3, 4, 5, 6, 7])
    expect(new Set(serverChangeLog(server.path).map(row => row.tx_id))).toEqual(new Set(['tx-big', 'tx-after']))

    const abandoned = sequenced([deviceChange({ id: 20, txId: 'tx-abandoned' })], 20, true)
    const moved = sequenced([deviceChange({ id: 21, txId: 'tx-moved-on' })], 21, false)
    expect((await push(server.base, abandoned)).status).toBe(200)
    expect([(await push(server.base, moved)).body]).toEqual([{ applied: 1, skipped: 0, conflicts: 0 }])
    expect(storedNotes(server.path).map(note => note.id)).toEqual([1, 2, 3, 4, 5, 6, 7, 21])
    expect(readSqlite(server.path, 'SELECT COUNT(*) AS n FROM _sirannon_staged_push_changes')).toEqual([{ n: 0n }])
  })

  it('refuses a malformed sequence on a change or a continuing transaction without sequences, and writes nothing', async () => {
    const server = await startDeviceServer()
    const batch = wireBatch([deviceChange({ id: 1 }), deviceChange({ id: 2 })], { fromSeq: '5' })
    const changes = batch.changes as Array<Record<string, unknown>>
    const withSeqs = (seqs: Array<string | undefined>, extra: Record<string, unknown> = {}) => ({
      ...batch,
      ...extra,
      changes: changes.map((change, at) => (seqs[at] === undefined ? change : { ...change, seq: seqs[at] })),
    })
    const malformed: Array<[string, Record<string, unknown>]> = [
      ['a sequence on only one change', withSeqs(['5', undefined])],
      ['sequences that fall', withSeqs(['6', '5'])],
      ['a first sequence other than fromSeq', withSeqs(['4', '6'])],
      ['a last sequence other than toSeq', withSeqs(['5', '7'])],
      ['a sequence past what SQLite stores', withSeqs(['5', '9223372036854775808'])],
      ['a numeric sequence', withSeqs(['5', 6 as unknown as string])],
      ['a continuing transaction with no sequences', withSeqs([undefined, undefined], { txContinues: true })],
      ['a string as txContinues', withSeqs(['5', '6'], { txContinues: 'yes' })],
    ]

    for (const [label, body] of malformed) {
      const answer = await push(server.base, body)
      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([400, 'INVALID_REQUEST'])
    }
    expect(storedNotes(server.path)).toEqual([])
    const sound = await push(server.base, withSeqs(['5', '6']))
    expect([sound.status, sound.body]).toEqual([200, { applied: 2, skipped: 0, conflicts: 0 }])
  })

  it('answers a batch declaring a sequence range far wider than its changes at once, and keeps the server writable', async () => {
    const server = await startDeviceServer()
    const batch = wireBatch([deviceChange({ id: 1 })], { fromSeq: '1', toSeq: '9999999999999999999' })

    const largestStorable = '9223372036854775807'
    const wide = wireBatch([deviceChange({ id: 3 }), deviceChange({ id: 4 })], {
      fromSeq: '10',
      toSeq: largestStorable,
    })

    const answer = await within(push(server.base, batch), 'the push of a wide sequence range')
    const accepted = await within(push(server.base, wide), 'the push of the widest storable range')
    const replay = await within(push(server.base, wide), 'the replay of the widest storable range')
    await within(server.db.execute("INSERT INTO notes (id, body) VALUES (2, 'still writable')"), 'a later write')

    expect([answer.status, errorCodeOf(answer.body)]).toEqual([400, 'INVALID_REQUEST'])
    expect([accepted.status, accepted.body]).toEqual([200, { applied: 2, skipped: 0, conflicts: 0 }])
    expect([replay.status, replay.body]).toEqual([200, { applied: 0, skipped: 2, conflicts: 0 }])
    expect(storedNotes(server.path).map(note => note.id)).toEqual([2, 3, 4])
    expect(readSqlite(server.path, 'SELECT COUNT(*) AS n FROM _sirannon_applied_changes')).toEqual([{ n: 2n }])
  })
})
