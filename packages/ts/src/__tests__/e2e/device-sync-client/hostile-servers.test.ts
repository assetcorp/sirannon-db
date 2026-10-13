import { afterEach, describe, expect, it } from 'vitest'
import type { SnapshotOutcome } from '../../../client/index.js'
import type { Migration } from '../../../core/index.js'
import { readSqlite, within } from '../lib/served-registry.js'
import { countOf, eventually, openDevice, releaseDeviceSync, sameRows, sleep, startSyncServer } from './harness.js'
import { forwardEverything, type HostileAnswer, type HostileRequest, startHostileServer } from './hostile-server.js'

const REQUIRED_CAPABILITIES = [
  'sync.push',
  'sync.echo-suppression',
  'sync.ack',
  'sync.resume',
  'sync.snapshot',
  'sync.migrations',
  'sync.schema-gate',
  'sync.stream-apply',
]
const REQUEST_LIMIT_MS = 400
const OUTAGE_CODES = ['CONNECTION_ERROR', 'INVALID_RESPONSE', 'TIMEOUT']

afterEach(releaseDeviceSync)

function on(path: string, answer: HostileAnswer): (request: HostileRequest) => HostileAnswer {
  return request => (request.path.endsWith(path) ? answer : forwardEverything())
}

describe('a device facing a server that answers badly', () => {
  it('refuses to start against a server that predates device sync, keeps it off, or lacks a capability, naming each missing capability', async () => {
    const server = await startSyncServer()
    const closed = await startSyncServer({ options: { acceptDeviceSync: false } })
    const cases: Array<[label: string, url: string, code: string, names: string[]]> = [
      [
        'no capability route',
        (
          await startHostileServer(
            () => server.directPort,
            on('/capabilities', {
              kind: 'json',
              status: 404,
              body: { error: { code: 'NOT_FOUND', message: 'Route not found' } },
            }),
          )
        ).url,
        'SYNC_UNSUPPORTED',
        [],
      ],
      [
        'one capability',
        (
          await startHostileServer(
            () => server.directPort,
            on('/capabilities', { kind: 'json', status: 200, body: { capabilities: ['sync.push'] } }),
          )
        ).url,
        'SYNC_UNSUPPORTED',
        REQUIRED_CAPABILITIES.slice(1),
      ],
      [
        'all but one capability',
        (
          await startHostileServer(
            () => server.directPort,
            on('/capabilities', {
              kind: 'json',
              status: 200,
              body: { capabilities: REQUIRED_CAPABILITIES.slice(0, -1) },
            }),
          )
        ).url,
        'SYNC_UNSUPPORTED',
        ['sync.stream-apply'],
      ],
      [
        'no device capability',
        (
          await startHostileServer(
            () => server.directPort,
            on('/capabilities', { kind: 'json', status: 200, body: { capabilities: ['query.named'] } }),
          )
        ).url,
        'DEVICE_SYNC_NOT_ACCEPTED',
        [],
      ],
      ['device sync turned off', closed.url, 'DEVICE_SYNC_NOT_ACCEPTED', []],
    ]

    for (const [label, url, code, names] of cases) {
      const device = await openDevice(url)
      await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'never pushed')")
      const controller = device.sync()
      const refusal = await controller.start().then(
        () => null,
        (err: unknown) => err as { code?: string; message?: string },
      )
      expect(refusal?.code, label).toBe(code)
      for (const name of names) expect(refusal?.message, label).toContain(name)
      expect((await controller.status()).state, label).toBe('stopped')
    }
    await sleep(200)
    expect(countOf(server.path, 'notes')).toBe(0)
    expect(countOf(closed.path, 'notes')).toBe(0)
  })

  it('treats a malformed, cut, or stalled capability answer as an outage and syncs both ways regardless', async () => {
    const server = await startSyncServer()
    const answers: HostileAnswer[] = [
      { kind: 'text', status: 200, text: 'capabilities: all of them' },
      { kind: 'json', status: 200, body: { capabilities: 'sync.push' } },
      { kind: 'json', status: 200, body: { capabilities: [1, 2] } },
      { kind: 'json', status: 200, body: { capabilities: REQUIRED_CAPABILITIES, registry: { digest: 5 } } },
      { kind: 'json', status: 200, body: null },
      { kind: 'cut', status: 200, text: JSON.stringify({ capabilities: REQUIRED_CAPABILITIES }) },
      { kind: 'stall', status: 200, text: JSON.stringify({ capabilities: REQUIRED_CAPABILITIES }) },
    ]

    for (const [index, answer] of answers.entries()) {
      const hostile = await startHostileServer(() => server.directPort, on('/capabilities', answer))
      const device = await openDevice(hostile.url)
      const id = (index + 1) * 10
      await device.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `device ${index}`])
      const controller = device.sync({ requestTimeout: REQUEST_LIMIT_MS })
      await within(controller.start(), `start against answer ${index}`, REQUEST_LIMIT_MS * 5)

      const degraded = await controller.status()
      expect([degraded.state, degraded.serverCapabilities], `answer ${index}`).toEqual(['running', null])
      expect(OUTAGE_CODES, `answer ${index}`).toContain(degraded.lastError?.code)
      await eventually(
        () => readSqlite(server.path, 'SELECT 1 FROM notes WHERE id = ?', [id]).length === 1,
        `push ${index}`,
      )
      await server.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id + 1, `server ${index}`])
      await eventually(
        () => readSqlite(device.path, 'SELECT 1 FROM notes WHERE id = ?', [id + 1]).length === 1,
        `pull ${index}`,
      )
      await controller.stop()
    }
  }, 60_000)

  it('starts within its request limit against a server that never completes the upgrade, and pulls once upgrades succeed', async () => {
    const server = await startSyncServer()
    let holding = true
    const hostile = await startHostileServer(() => server.directPort, forwardEverything, {
      holdUpgrades: () => holding,
    })
    const device = await openDevice(hostile.url)
    const controller = device.sync({ requestTimeout: REQUEST_LIMIT_MS })

    await within(controller.start(), 'the start against a held upgrade', REQUEST_LIMIT_MS * 5)
    const held = await controller.status()
    expect(held.state).toBe('running')
    expect(OUTAGE_CODES).toContain(held.lastError?.code)
    await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'pushed over HTTP while the pull waits')")
    await eventually(() => countOf(server.path, 'notes') === 1, 'the push')

    holding = false
    await eventually(async () => (await controller.status()).lastPulledSeq !== null, 'the first subscription')
    await server.db.execute("INSERT INTO notes (id, body) VALUES (2, 'pulled once the upgrade succeeds')")
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the pull after the upgrade succeeds')

    holding = true
    const upgradesBeforeDrop = hostile.upgradeCount()
    hostile.dropUpgrades()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (3, 'written while upgrades hang again')")
    await eventually(() => hostile.upgradeCount() > upgradesBeforeDrop + 1, 'reconnects timing out')
    expect(countOf(device.path, 'notes')).toBe(2)
    holding = false
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the resumed pull')
  }, 30_000)

  it('keeps its data when a snapshot manifest is malformed, and blocks it only when a page fails after the wipe', async () => {
    const server = await startSyncServer()
    for (let id = 1; id <= 5; id++)
      await server.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'server'])
    const manifest = {
      databaseId: 'app',
      startSeq: '5',
      epoch: 'a'.repeat(32),
      schema: ['CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)'],
      tables: [{ name: 'notes', rowCount: 1 }],
      migrations: [],
    }
    const badManifests: unknown[] = [
      { ...manifest, tables: [{ name: 'notes"; DROP TABLE notes; --', rowCount: 1 }] },
      { ...manifest, tables: [{ name: '_sirannon_meta', rowCount: 1 }] },
      { ...manifest, startSeq: 'twelve' },
      { ...manifest, startSeq: '1'.repeat(20) },
      { ...manifest, epoch: '' },
      { ...manifest, schema: ["ATTACH DATABASE '/tmp/stolen.db' AS stolen"] },
      { ...manifest, schema: ['CREATE TABLE notes (id INTEGER PRIMARY KEY); DROP TABLE notes'] },
      { ...manifest, schema: ['CREATE TABLE _sirannon_meta (key TEXT)'] },
      { ...manifest, migrations: [{ version: 'one', name: 'x', checksum: null }] },
      { ...manifest, tables: 'notes' },
    ]
    const badPages: unknown[] = [
      { rows: [{ id: 1, body: 'tampered' }], checksum: '0'.repeat(64), done: true, nextKey: null },
      { rows: [], checksum: '', done: false, nextKey: [1] },
      { rows: [{ id: 1, body: 'x' }], checksum: 'x', done: false, nextKey: null },
      { rows: 'all of them', checksum: '', done: true, nextKey: null },
    ]

    const runs: Array<[label: string, answer: HostileAnswer, path: string, usable: boolean]> = [
      ...badManifests.map((body, index): [string, HostileAnswer, string, boolean] => [
        `manifest ${index}`,
        { kind: 'json', status: 200, body },
        '/snapshot',
        true,
      ]),
      ...badPages.map((body, index): [string, HostileAnswer, string, boolean] => [
        `page ${index}`,
        { kind: 'json', status: 200, body },
        '/snapshot/page',
        false,
      ]),
    ]

    for (const [label, answer, path, usable] of runs) {
      let hostileAnswer = true
      const hostile = await startHostileServer(
        () => server.directPort,
        request => (hostileAnswer && request.path.endsWith(path) ? answer : forwardEverything()),
      )
      const device = await openDevice(hostile.url)
      await device.db.execute("INSERT INTO notes (id, body) VALUES (100, 'local only')")
      const outcomes: SnapshotOutcome[] = []
      const controller = device.sync({ autoResync: false, onSnapshotComplete: outcome => outcomes.push(outcome) })
      await controller.start()
      await eventually(() => countOf(server.path, 'notes') >= 6, `${label}: the local row reaching the server`)
      controller.pause()

      const failure = await within(
        controller.downloadSnapshot().then(
          () => null,
          (err: unknown) => err as { code?: string },
        ),
        `${label}: the failed download`,
      )
      expect(failure?.code, label).toMatch(/^(INVALID_RESPONSE|SNAPSHOT_CHECKSUM_MISMATCH)$/)
      expect(
        outcomes.map(outcome => outcome.databaseUsable),
        label,
      ).toEqual([usable])
      if (usable) {
        expect(readSqlite(device.path, 'SELECT id FROM notes'), label).toEqual([{ id: 100n }])
        expect(readSqlite(server.path, 'SELECT name FROM sqlite_master WHERE name = ?', ['notes']), label).toHaveLength(
          1,
        )
      }

      hostileAnswer = false
      await controller.downloadSnapshot()
      expect(sameRows(server.path, device.path, 'notes'), label).toBe(true)
      await controller.stop()
    }
  }, 120_000)

  it('pushes a write again when the answer to its push is lost or malformed, and the server keeps it once', async () => {
    const server = await startSyncServer()
    const answers: HostileAnswer[] = [
      { kind: 'swallow' },
      { kind: 'rewrite', rewrite: () => '{"applied":"1","skipped":0,"conflicts":0}' },
      { kind: 'rewrite', rewrite: () => 'applied' },
      { kind: 'stall', status: 200, text: '{"applied":1,"skipped":0,"conflicts":0}' },
    ]

    for (const [index, answer] of answers.entries()) {
      let hostileAnswer = true
      const hostile = await startHostileServer(
        () => server.directPort,
        request => (hostileAnswer && request.path.endsWith('/changes') ? answer : forwardEverything()),
      )
      const device = await openDevice(hostile.url)
      const controller = device.sync({ requestTimeout: REQUEST_LIMIT_MS })
      await controller.start()
      const id = (index + 1) * 10
      await device.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `answer ${index}`])

      await eventually(async () => OUTAGE_CODES.includes((await controller.status()).lastError?.code ?? ''), `${index}`)
      expect((await controller.status()).pendingPushCount, `answer ${index}`).toBe(1)
      hostileAnswer = false
      await eventually(async () => (await controller.status()).pushCaughtUp, `the push after answer ${index}`)

      const [logged] = readSqlite<{ n: bigint }>(
        server.path,
        "SELECT count(*) AS n FROM _sirannon_changes WHERE table_name = 'notes' AND row_id = ?",
        [String(id)],
      )
      expect(Number(logged?.n), `answer ${index}`).toBe(1)
      expect(readSqlite(server.path, 'SELECT body FROM notes WHERE id = ?', [id]), `answer ${index}`).toEqual([
        { body: `answer ${index}` },
      ])
      await controller.stop()
    }
  }, 60_000)

  it('refuses to apply migration SQL that does not match its checksum and takes the schema from a snapshot', async () => {
    const createNotes: Migration = {
      version: 1,
      name: 'create_notes',
      up: 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)',
    }
    const addFlag: Migration = { version: 2, name: 'add_flag', up: 'ALTER TABLE notes ADD COLUMN flag INTEGER' }
    const server = await startSyncServer({ migrations: [createNotes, addFlag] })
    await server.db.execute("INSERT INTO notes (id, body, flag) VALUES (1, 'server', 1)")
    const hostile = await startHostileServer(
      () => server.directPort,
      on('/migrations', {
        kind: 'rewrite',
        rewrite: body => body.replace('ADD COLUMN flag INTEGER', 'ADD COLUMN planted INTEGER'),
      }),
    )
    const device = await openDevice(hostile.url, { migrations: [createNotes] })
    const announced: string[] = []
    const controller = device.sync({ snapshotRetryDelayMs: 50, onResyncRequired: () => announced.push('resync') })
    await controller.start()

    await eventually(async () => {
      const status = await controller.status()
      return status.state === 'running' && !status.resyncRequired && status.schemaVersion === 2
    }, 'the snapshot replacing the schema')
    expect(announced).toEqual(['resync'])
    expect(readSqlite(device.path, "SELECT name FROM pragma_table_info('notes') WHERE name = 'planted'")).toEqual([])
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
  })
})
