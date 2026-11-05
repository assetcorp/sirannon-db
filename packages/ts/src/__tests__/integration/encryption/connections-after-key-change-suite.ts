import { randomBytes } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { LiveQuery } from '../../../core/live/types.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { ChangeEvent } from '../../../core/types.js'

const POLL_MS = 5
const WAIT_STEP_MS = 5
const WAIT_LIMIT_MS = 8_000
const NOTES_BEFORE_CHANGE = 20
const NOTES_AFTER_CHANGE = 10
const FOLDERS_AFTER_CHANGE = 5

type Row = Record<string, unknown>
type PushedChange = [string, string, Row, Row | null]

function keyFunction(bytes: Uint8Array, id: string) {
  return () => ({ key: Uint8Array.from(bytes), id })
}

async function waitFor(condition: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + WAIT_LIMIT_MS
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await new Promise(resolve => setTimeout(resolve, WAIT_STEP_MS))
  }
}

function liveRows(query: LiveQuery<Row>): Row[] | null {
  const state = query.getState()
  return state.status === 'ready' && !state.revalidating ? state.rows.map(row => ({ ...row })) : null
}

function plainNumbers(row: Row): Row {
  return Object.fromEntries(
    Object.entries(row).map(([column, value]) => [column, typeof value === 'bigint' ? Number(value) : value]),
  )
}

export function describeConnectionsAfterKeyChange(label: string, newSirannon: () => Sirannon): void {
  describe(`the connections that a database reopens after a key change on ${label}`, () => {
    let dir: string
    let sirannon: Sirannon

    beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), 'sirannon-reopened-'))
      sirannon = newSirannon()
    })

    afterEach(async () => {
      await sirannon.shutdown()
      rmSync(dir, { recursive: true, force: true })
    })

    async function openNotes(name: string): Promise<Database> {
      const db = await sirannon.open(name, join(dir, `${name}.db`), {
        encryption: { key: keyFunction(randomBytes(32), 'first') },
        cdcPollInterval: POLL_MS,
      })
      await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
      return db
    }

    it('pushes every change from the device outbox after a key change, including a table that sync first meets after it', async () => {
      const db = await openNotes('device')
      await db.watch('notes')
      const port = db.deviceSync()
      const { nodeId } = await port.identity()
      const written: PushedChange[] = []
      const insertNote = async (id: number) => {
        await db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `note ${id}`])
        written.push(['notes', 'insert', { id }, { id, body: `note ${id}` }])
      }
      for (let id = 1; id <= NOTES_BEFORE_CHANGE; id++) await insertNote(id)
      const pushed = (changes: readonly { table: string; operation: string; primaryKey: Row; newData: Row | null }[]) =>
        changes.map(change => [
          change.table,
          change.operation,
          plainNumbers(change.primaryKey),
          change.newData === null ? null : plainNumbers(change.newData),
        ])

      const before = await port.readOutboxBatch(0n, 1_000)
      expect(before).not.toBeNull()
      if (before === null) return
      expect(pushed(before.changes)).toEqual(written)
      written.length = 0

      const status = await db.changeEncryption({ key: keyFunction(randomBytes(32), 'second') })
      expect(status.change.state).toBe('done')
      await db.execute('CREATE TABLE folders (code TEXT PRIMARY KEY, title TEXT NOT NULL)')
      await db.watch('folders')
      for (let at = 1; at <= FOLDERS_AFTER_CHANGE; at++) {
        await db.execute('INSERT INTO folders (code, title) VALUES (?, ?)', [`F-${at}`, `folder ${at}`])
        written.push(['folders', 'insert', { code: `F-${at}` }, { code: `F-${at}`, title: `folder ${at}` }])
      }
      for (let id = NOTES_BEFORE_CHANGE + 1; id <= NOTES_BEFORE_CHANGE + NOTES_AFTER_CHANGE; id++) await insertNote(id)
      await db.execute('DELETE FROM notes WHERE id = 1')
      written.push(['notes', 'delete', { id: 1 }, null])

      const after = await port.readOutboxBatch(before.toSeq, 1_000)
      expect(after).not.toBeNull()
      if (after === null) return
      expect(after.sourceNodeId).toBe(nodeId)
      expect(pushed(after.changes)).toEqual(written)
      expect(await port.countOutboxPending(before.toSeq)).toBe(written.length)
    })

    it('keeps a live query and a change subscription current across a key change', async () => {
      const db = await openNotes('live')
      for (let id = 1; id <= 3; id++) await db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `note ${id}`])
      const expected = new Map<number, string>([1, 2, 3].map(id => [id, `note ${id}`]))
      const query = await db.live<Row>('SELECT id, body FROM notes ORDER BY id', undefined, { rereadJitterMs: 0 })
      const delivered: number[] = []
      const failures: Error[] = []
      const subscription = db.on('notes').subscribe(
        (event: ChangeEvent<Row>) => {
          delivered.push(Number(event.row.id))
        },
        { onError: err => failures.push(err) },
      )
      const matchesExpected = () =>
        JSON.stringify(liveRows(query)?.map(plainNumbers)) ===
        JSON.stringify([...expected].map(([id, body]) => ({ id, body })))
      try {
        await waitFor(matchesExpected, 'the live query to hold the first rows')
        await db.execute('INSERT INTO notes (id, body) VALUES (4, ?)', ['note 4'])
        expected.set(4, 'note 4')
        await waitFor(() => delivered.includes(4) && matchesExpected(), 'the change before the key change')

        const status = await db.changeEncryption({ key: keyFunction(randomBytes(32), 'second') })
        expect(status.change.state).toBe('done')
        for (const id of [5, 6]) {
          await db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `note ${id}`])
          expected.set(id, `note ${id}`)
        }
        await db.execute("UPDATE notes SET body = 'note 2, revised' WHERE id = 2")
        expected.set(2, 'note 2, revised')

        await waitFor(matchesExpected, 'the live query to hold the rows written after the key change')
        await waitFor(
          () => delivered.includes(5) && delivered.includes(6) && delivered.includes(2),
          'the changes after the key change',
        )
        expect(failures).toEqual([])
        expect(delivered).toEqual([4, 5, 6, 2])
      } finally {
        subscription.unsubscribe()
        await query.close()
      }
    })
  })
}
