import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { ChangeEvent } from '../../../core/types.js'
import { postJson, serveRegistry, trackedRegistry } from '../lib/served-registry.js'
import {
  cdcEpochOf,
  changeLog,
  highestSeq,
  pause,
  QUIET_PERIOD_MS,
  releaseChangeStreams,
  type ServedDatabase,
  serveDatabase,
  WireSocket,
  waitFor,
  writeFile,
} from './harness.js'

const NOTES_SCHEMA = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)'
const INSERT_NOTE = 'INSERT INTO notes (body) VALUES (?)'
const HISTORY = ['one', 'two', 'three', 'four', 'five', 'six']
const RECONNECT_INTERVAL_MS = 50
const CURSOR_AHEAD_OF_THE_LOG = '99999999999999999999'

afterEach(releaseChangeStreams)

async function notes(label: string): Promise<ServedDatabase & { epoch: string }> {
  const served = await serveDatabase({ label, databaseId: 'notes', schema: [NOTES_SCHEMA] })
  await served.db.watch('notes')
  for (const body of HISTORY) await served.db.execute(INSERT_NOTE, [body])
  await served.db.execute("UPDATE notes SET body = 'TWO' WHERE id = 2")
  const first = await WireSocket.open(`${served.served.wsBase}/db/notes`)
  const subscribed = await first.request({ id: 'first', type: 'subscribe', table: 'notes' })
  first.close()
  await first.closed
  return { ...served, epoch: subscribed.epoch ?? '' }
}

async function writeNote(served: ServedDatabase, body: string): Promise<void> {
  const answer = await postJson(served.served.base, '/db/notes/execute', { sql: INSERT_NOTE, params: [body] })
  expect(answer.status, answer.text).toBe(200)
}

function loggedAfter(path: string, cursor: bigint): string[] {
  return changeLog(path, 'notes')
    .filter(row => row.seq > cursor)
    .map(row => row.seq.toString())
}

describe('resuming a subscription from a cursor', () => {
  it('replays every retained change after the cursor, then streams live changes, with no gap and no repeat', async () => {
    const served = await notes('resume-replay')
    const log = changeLog(served.path, 'notes')
    const epoch = served.epoch
    const cursors = ['0', log[2]?.seq.toString() ?? '', highestSeq(served.path).toString()]

    const resumed = await Promise.all(
      cursors.map(async (cursor, index) => {
        const socket = await WireSocket.open(`${served.served.wsBase}/db/notes`)
        socket.send({ id: 'feed', type: 'subscribe', table: 'notes', sinceSeq: cursor, epoch })
        await writeNote(served, `during-${index}`)
        return { socket, cursor }
      }),
    )
    for (const body of ['after-1', 'after-2']) await writeNote(served, body)
    const total = changeLog(served.path, 'notes').length
    await waitFor(
      () =>
        resumed.every(
          ({ socket, cursor }) => socket.events('feed').length >= loggedAfter(served.path, BigInt(cursor)).length,
        ),
      'every resumed subscription to catch up',
    )
    await pause(QUIET_PERIOD_MS)

    expect(total).toBe(HISTORY.length + 1 + cursors.length + 2)
    expect(epoch).toBe(cdcEpochOf(served.path))
    for (const { socket, cursor } of resumed) {
      const [subscribed] = socket.of('feed', 'subscribed')
      expect(subscribed, cursor).toMatchObject({ epoch })
      expect(subscribed?.resync, cursor).toBeUndefined()
      expect(
        socket.events('feed').map(event => event.seq),
        cursor,
      ).toEqual(loggedAfter(served.path, BigInt(cursor)))
    }
    expect(
      resumed[0]?.socket
        .events('feed')
        .slice(0, 7)
        .map(event => event.row.body ?? event.oldRow?.body),
    ).toEqual([...HISTORY, 'TWO'])
  })

  it('answers with resync and replays nothing it cannot vouch for when the epoch is foreign, the log was pruned, or a retained change is unreadable', async () => {
    const foreign = await notes('resume-foreign')
    const pruned = await notes('resume-pruned')
    const corrupt = await notes('resume-corrupt')
    const prunedLog = changeLog(pruned.path, 'notes')
    const tail = prunedLog[prunedLog.length - 2]?.seq ?? 0n
    writeFile(pruned.path, 'DELETE FROM _sirannon_changes WHERE seq < ?', [tail])
    const unreadable = changeLog(corrupt.path, 'notes')[3]?.seq ?? 0n
    writeFile(corrupt.path, "UPDATE _sirannon_changes SET new_data = '{' WHERE seq = ?", [unreadable])
    const cases: Array<[label: string, served: ServedDatabase, epoch: string]> = [
      ['a cursor from another file', foreign, pruned.epoch],
      ['a cursor older than the retained log', pruned, pruned.epoch],
      ['a retained change that is not valid JSON', corrupt, corrupt.epoch],
    ]
    for (const [label, served, epoch] of cases) {
      const socket = await WireSocket.open(`${served.served.wsBase}/db/notes`)
      socket.send({ id: 'feed', type: 'subscribe', table: 'notes', sinceSeq: '0', epoch })
      await waitFor(
        () => socket.of('feed', 'subscribed').some(message => message.resync === true),
        `resync for ${label}`,
      )
      await pause(QUIET_PERIOD_MS)
      const beforeLive = socket.events('feed').length
      await writeNote(served, `live after ${label}`)
      const events = await socket.waitForEvents('feed', beforeLive + 1)

      expect(beforeLive, label).toBe(0)
      expect(
        events.map(event => event.row.body),
        label,
      ).toEqual([`live after ${label}`])
      expect(socket.isOpen, label).toBe(true)
    }
    expect([foreign.epoch, pruned.epoch, corrupt.epoch]).toEqual([
      cdcEpochOf(foreign.path),
      cdcEpochOf(pruned.path),
      cdcEpochOf(corrupt.path),
    ])
    expect(new Set([foreign.epoch, pruned.epoch, corrupt.epoch]).size).toBe(3)
  })

  it('answers a cursor ahead of the log with resync and delivers every live change after it, and trusts a caught-up cursor once pruning empties the log', async () => {
    const served = await notes('resume-ahead')
    const caughtUpCursor = highestSeq(served.path).toString()
    writeFile(served.path, 'DELETE FROM _sirannon_changes')
    const caughtUp = await WireSocket.open(`${served.served.wsBase}/db/notes`)
    const caughtUpSubscribed = await caughtUp.request({
      id: 'feed',
      type: 'subscribe',
      table: 'notes',
      sinceSeq: caughtUpCursor,
      epoch: served.epoch,
    })
    const socket = await WireSocket.open(`${served.served.wsBase}/db/notes`)
    const subscribed = await socket.request({
      id: 'feed',
      type: 'subscribe',
      table: 'notes',
      sinceSeq: CURSOR_AHEAD_OF_THE_LOG,
      epoch: served.epoch,
    })
    const foreign = await WireSocket.open(`${served.served.wsBase}/db/notes`)
    const foreignSubscribed = await foreign.request({
      id: 'feed',
      type: 'subscribe',
      table: 'notes',
      sinceSeq: CURSOR_AHEAD_OF_THE_LOG,
      epoch: 'an epoch from another file',
    })
    await writeNote(served, 'after a cursor from the future')
    const events = await socket.waitForEvents('feed', 1)
    const foreignEvents = await foreign.waitForEvents('feed', 1)
    const caughtUpEvents = await caughtUp.waitForEvents('feed', 1)
    const live = loggedAfter(served.path, BigInt(subscribed.seq ?? '0'))

    expect([subscribed.resync, foreignSubscribed.resync, caughtUpSubscribed.resync]).toEqual([true, true, undefined])
    expect(live).toHaveLength(1)
    expect(BigInt(live[0] ?? '0')).toBe(BigInt(caughtUpCursor) + 1n)
    expect(events.map(event => [event.seq, event.row.body])).toEqual([[live[0], 'after a cursor from the future']])
    expect(foreignEvents.map(event => event.seq)).toEqual(live)
    expect(caughtUpEvents.map(event => event.seq)).toEqual(live)
  })

  it('resumes the published client across server restarts, delivering each change once and in order, and reports a reset when the log no longer reaches its cursor', async () => {
    const served = await notes('resume-restart')
    const port = served.served.port
    const client = new SirannonClient(served.served.base, { reconnectInterval: RECONNECT_INTERVAL_MS })
    const received: ChangeEvent[] = []
    let resets = 0
    await client
      .database('notes')
      .on('notes')
      .subscribe(event => received.push(event), { onReset: () => resets++ })
    await writeNote(served, 'before the restart')
    await waitFor(() => received.length === 1, 'the first change')

    await served.served.server.close()
    await served.sirannon.shutdown()
    const second = trackedRegistry()
    const reopened = await second.open('notes', served.path)
    await reopened.execute(INSERT_NOTE, ['while the server was down'])
    await reopened.execute("UPDATE notes SET body = 'edited while down' WHERE id = 1")
    const again = await serveRegistry(second, { acceptSql: true, port })
    await waitFor(() => received.length === 3, 'the changes made while the server was down')
    await postJson(again.base, '/db/notes/execute', { sql: INSERT_NOTE, params: ['after the restart'] })
    await waitFor(() => received.length === 4, 'the change after the restart')
    const firstLife = changeLog(served.path, 'notes').filter(row => row.seq > BigInt(HISTORY.length + 1))

    await again.server.close()
    await second.shutdown()
    writeFile(served.path, "INSERT INTO notes (body) VALUES ('lost one')")
    writeFile(served.path, "INSERT INTO notes (body) VALUES ('lost two')")
    writeFile(served.path, 'DELETE FROM _sirannon_changes WHERE seq < ?', [highestSeq(served.path)])
    const third = trackedRegistry()
    await third.open('notes', served.path)
    const last = await serveRegistry(third, { acceptSql: true, port })
    await waitFor(() => resets === 1, 'a reset')
    await postJson(last.base, '/db/notes/execute', { sql: INSERT_NOTE, params: ['after the reset'] })
    await waitFor(() => received.length === 5, 'the change after the reset')
    await pause(QUIET_PERIOD_MS)
    client.close()

    expect(received.slice(0, 4).map(event => event.seq)).toEqual(firstLife.map(row => row.seq))
    expect(received.map(event => [event.type, event.row.body ?? event.oldRow?.body])).toEqual([
      ['insert', 'before the restart'],
      ['insert', 'while the server was down'],
      ['update', 'edited while down'],
      ['insert', 'after the restart'],
      ['insert', 'after the reset'],
    ])
    expect(resets).toBe(1)
  })
})
