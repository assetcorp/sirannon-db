import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Sirannon } from '../../../core/sirannon.js'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import {
  RawSocket,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  sendRaw,
  serveRegistry,
} from '../lib/served-registry.js'
import {
  billedAs,
  Meter,
  meteredRegistry,
  rowInserts,
  rowsIn,
  SIRANNON_SQLITE_BINDING,
  TOPOLOGIES,
} from './metered-registry.js'

const MAX_BODY_BYTES = 96 * 1024
const INSERT_NOTE = 'INSERT INTO notes (body) VALUES (?)'
const HTTP_SETS = 2_000
const WS_SETS = 1_500

let meter: Meter
let sirannon: Sirannon
let served: ServedRegistry
let notesPath: string

function bodyOfExactly(bytes: number, sets: number, envelope: Record<string, unknown> = {}): string {
  const paramsBatch = Array.from({ length: sets }, (_, index) => [`note-${index}`])
  const unpadded = Buffer.byteLength(JSON.stringify({ ...envelope, sql: INSERT_NOTE, paramsBatch }))
  paramsBatch[sets - 1] = [`note-${sets - 1}${'x'.repeat(bytes - unpadded)}`]
  const body = JSON.stringify({ ...envelope, sql: INSERT_NOTE, paramsBatch })
  expect(Buffer.byteLength(body)).toBe(bytes)
  return body
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering the largest requests that the server accepts', () => {
  afterEach(releaseServedRegistries)

  describe.each(TOPOLOGIES)('with $label', topology => {
    beforeEach(async () => {
      const dir = scratchDirectory('metered-limits')
      meter = new Meter()
      sirannon = meteredRegistry(meter)
      notesPath = join(dir, 'notes.db')
      const db = await sirannon.open('notes', notesPath, topology.options)
      await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
      await db.execute('CREATE INDEX notes_by_body ON notes (body)')
      served = await serveRegistry(sirannon, { acceptSql: true, maxBodyBytes: MAX_BODY_BYTES })
      meter.clear()
    })

    it('bills a batch and a load whose bodies fill maxBodyBytes exactly, and reports nothing for a body one byte over', async () => {
      const billPerRequest = await rowInserts(notesPath, 'notes', HTTP_SETS)
      const batch = await sendRaw(served.base, 'POST', '/db/notes/batch', bodyOfExactly(MAX_BODY_BYTES, HTTP_SETS))
      const load = await sendRaw(served.base, 'POST', '/db/notes/load', bodyOfExactly(MAX_BODY_BYTES, HTTP_SETS))
      const overBatch = await sendRaw(
        served.base,
        'POST',
        '/db/notes/batch',
        bodyOfExactly(MAX_BODY_BYTES + 1, HTTP_SETS),
      )
      const overLoad = await sendRaw(
        served.base,
        'POST',
        '/db/notes/load',
        bodyOfExactly(MAX_BODY_BYTES + 1, HTTP_SETS),
      )

      expect([batch.status, load.status, overBatch.status, overLoad.status]).toEqual([200, 200, 413, 413])
      expect(await rowsIn(notesPath, 'notes')).toBe(HTTP_SETS * 2)
      expect(meter.billed('notes')).toEqual([
        billedAs(INSERT_NOTE, false, billPerRequest),
        billedAs(INSERT_NOTE, false, billPerRequest),
      ])
    })

    it('bills a WebSocket batch and load that fill maxBodyBytes exactly, and nothing for a message one byte over, which closes the socket', async () => {
      const billPerMessage = await rowInserts(notesPath, 'notes', WS_SETS)
      const ws = await RawSocket.open(`${served.wsBase}/db/notes`)
      const batch = ws.reply('batch-at-limit')
      ws.sendText(bodyOfExactly(MAX_BODY_BYTES, WS_SETS, { id: 'batch-at-limit', type: 'batch' }))
      expect((await batch).type).toBe('result')
      const load = ws.reply('load-at-limit')
      ws.sendText(bodyOfExactly(MAX_BODY_BYTES, WS_SETS, { id: 'load-at-limit', type: 'load' }))
      expect((await load).type).toBe('result')

      const overflowing = await RawSocket.open(`${served.wsBase}/db/notes`)
      overflowing.sendText(bodyOfExactly(MAX_BODY_BYTES + 1, WS_SETS, { id: 'over', type: 'batch' }))
      expect((await overflowing.closed).code).toBe(1009)
      expect(ws.open).toBe(true)

      expect(await rowsIn(notesPath, 'notes')).toBe(WS_SETS * 2)
      expect(meter.billed('notes')).toEqual([
        billedAs(INSERT_NOTE, false, billPerMessage),
        billedAs(INSERT_NOTE, false, billPerMessage),
      ])
    })
  })
})
