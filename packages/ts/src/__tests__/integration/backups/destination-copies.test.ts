import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { assembleFromDestination } from '../../../backup/index.js'
import type { BackupProgress } from '../../../core/backup/report.js'
import { COPY_STALL_MS, stallingCopyDriver } from '../../helpers/fault-drivers/stalling-copy-driver.js'
import { memoryDestination } from '../../helpers/memory-destination.js'
import {
  codeOf,
  createWorkspace,
  failureOf,
  filesIn,
  framesIn,
  INSERT_ORDER,
  integrityOf,
  joinPieces,
  logHeaderOf,
  ORDER_PADDING,
  orderLabels,
  PLACEMENTS,
  readRaw,
  seedOrders,
  sha256,
  type Workspace,
  waitFor,
  walBytes,
  watchUnhandledRejections,
} from './harness.js'

const PIECE_BYTES = 4096
const LONGEST_TIMER_MS = 2_147_483_647
const SLOW_STORE_MS = 300

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('destination-copies')
})

afterEach(async () => {
  await workspace.dispose()
})

function rebuilt(name: string, bytes: Uint8Array): string {
  const path = workspace.path(name)
  writeFileSync(path, bytes)
  return path
}

describe.each(PLACEMENTS)('copying a database to a destination with $label', ({ driver, writerWorker }) => {
  it('sends pieces that join into the same database and reports what it sent while it sent it', async () => {
    const sourcePath = workspace.path('orders.db')
    const db = await workspace.registry(driver()).open('orders', sourcePath, { writerWorker })
    await seedOrders(db, 'order', 500, ORDER_PADDING)
    await db.execute(INSERT_ORDER, ['written-after-the-load', ''])
    const destination = workspace.destination()
    const seen: BackupProgress[] = []

    const report = await db.backupTo({
      destination,
      name: 'orders-copy.db',
      pieceBytes: PIECE_BYTES,
      pagesPerStep: 4,
      stagingDir: workspace.path('staging'),
      onProgress: progress => seen.push({ ...progress }),
    })

    const joined = joinPieces(destination, 'orders-copy.db', PIECE_BYTES)
    const copy = rebuilt('joined.db', joined)
    const log = logHeaderOf(readFileSync(`${sourcePath}-wal`))
    expect(destination.names()).toEqual(['orders-copy.db'])
    expect(report).toMatchObject({
      databaseId: 'orders',
      sourcePath,
      kind: 'full',
      route: 'staged',
      destinationName: 'orders-copy.db',
      pieceBytes: PIECE_BYTES,
      restarts: 0,
    })
    expect(report.bytesWritten).toBe(joined.byteLength)
    expect(report.bytesWritten).toBe(report.pageCount * report.pageSize)
    expect(report.pieceCount).toBe(destination.indices('orders-copy.db').length)
    expect(report.pieceCount).toBe(Math.ceil(joined.byteLength / PIECE_BYTES))
    expect(report.fingerprint).toBe(sha256(joined))
    expect(report.logPosition).toEqual({
      logSequence: log.logSequence,
      salt1: log.salt1,
      salt2: log.salt2,
      lastFrame: framesIn(walBytes(sourcePath), log.pageSize),
    })
    expect(integrityOf(copy)).toBe('ok')
    expect(orderLabels(copy)).toEqual(orderLabels(sourcePath))

    const copySteps = seen.filter(progress => progress.phase === 'copy')
    const transfers = seen.filter(progress => progress.phase === 'transfer')
    expect(copySteps.length).toBeGreaterThanOrEqual(Math.ceil(report.pageCount / 4))
    expect(copySteps.at(-1)?.remainingPages).toBe(0)
    expect(transfers.map(progress => progress.piecesWritten)).toEqual(
      Array.from({ length: report.pieceCount }, (_, at) => at + 1),
    )
    expect(transfers.at(-1)?.bytesWritten).toBe(report.bytesWritten)
    expect(seen.every(progress => progress.runId === report.runId)).toBe(true)
    expect(filesIn(workspace.path('staging'))).toEqual([])
  })

  it('keeps taking writes for the whole copy with zero restarts', async () => {
    const db = await workspace.registry(driver()).open('busy', workspace.path('busy.db'), { writerWorker })
    await seedOrders(db, 'seeded', 20_000, ORDER_PADDING)
    let copying = true
    let writesDuringCopy = 0
    const writer = (async () => {
      while (copying) {
        await new Promise(settle => setImmediate(settle))
        if (!copying) break
        await db.execute(INSERT_ORDER, [`during-${writesDuringCopy}`, ''])
        writesDuringCopy++
      }
    })()
    const destination = workspace.destination()

    const report = await db.backupTo({ destination, name: 'busy.db', pieceBytes: 65_536, pagesPerStep: 8 })
    copying = false
    await writer

    const copy = rebuilt('busy-joined.db', joinPieces(destination, 'busy.db', 65_536))
    const labels = orderLabels(copy)
    expect(report.restarts).toBe(0)
    expect(writesDuringCopy).toBeGreaterThan(1)
    expect(integrityOf(copy)).toBe('ok')
    expect(labels.length).toBeGreaterThanOrEqual(20_000)
    expect(labels.slice(20_000)).toEqual(Array.from({ length: labels.length - 20_000 }, (_, at) => `during-${at}`))
  })

  it('leaves the fingerprint out when asked to, and still sends a copy that opens', async () => {
    const sourcePath = workspace.path('unprinted.db')
    const db = await workspace.registry(driver()).open('unprinted', sourcePath, { writerWorker })
    const labels = await seedOrders(db, 'order', 40)
    const destination = workspace.destination()

    const report = await db.backupTo({ destination, name: 'unprinted.db', fingerprint: false, pieceBytes: 1024 })

    expect(report.fingerprint).toBeUndefined()
    expect(orderLabels(rebuilt('unprinted-joined.db', joinPieces(destination, 'unprinted.db', 1024)))).toEqual(labels)
  })
})

describe('the size of the pieces a destination receives', () => {
  it('sends pieces of any positive whole size, down to one byte, and refuses a size that is not one', async () => {
    const db = await workspace.open('sizes')
    const labels = await seedOrders(db, 'order', 5)
    const fileBytes = (await db.backupTo({ destination: memoryDestination(), name: 'probe.db' })).bytesWritten

    for (const pieceBytes of [1, 511, 512, 4096, fileBytes - 1, fileBytes, fileBytes + 1, 16 * 1024 * 1024]) {
      const destination = memoryDestination()
      const report = await db.backupTo({ destination, name: 'sized.db', pieceBytes })
      const joined = destination.bytesFor('sized.db')
      expect(report.pieceCount, `${pieceBytes}`).toBe(Math.ceil(fileBytes / pieceBytes))
      expect(sha256(joined), `${pieceBytes}`).toBe(report.fingerprint)
      expect(orderLabels(rebuilt(`sized-${pieceBytes}.db`, joined)), `${pieceBytes}`).toEqual(labels)
    }
    for (const pieceBytes of [0, -1, -4096, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      const destination = memoryDestination()
      const error = await failureOf(db.backupTo({ destination, name: 'refused.db', pieceBytes }))
      expect(codeOf(error), `${pieceBytes}`).toBe('BACKUP_ERROR')
      expect(destination.names(), `${pieceBytes}`).toEqual([])
    }
  })
})

describe('a destination that fails, hangs, or holds pieces another run left', () => {
  it('names the piece the destination refused and removes the staged file whether the copy succeeds or fails', async () => {
    const db = await workspace.open('refused')
    await seedOrders(db, 'order', 300, ORDER_PADDING)
    const stagingDir = workspace.path('staging')
    mkdirSync(stagingDir)
    const destination = workspace.destination()

    await db.backupTo({ destination, name: 'accepted.db', pieceBytes: PIECE_BYTES, stagingDir })
    expect(filesIn(stagingDir)).toEqual([])

    destination.faults.refuseWrite = (name, index) => name === 'refused.db' && index === 2
    const error = await failureOf(db.backupTo({ destination, name: 'refused.db', pieceBytes: PIECE_BYTES, stagingDir }))

    expect(codeOf(error)).toBe('BACKUP_DESTINATION_ERROR')
    expect(error.message).toContain("piece 2 of 'refused.db'")
    expect(error.message).toContain('the bucket refused piece 2')
    expect(destination.indices('refused.db')).toEqual([0, 1])
    expect(filesIn(stagingDir)).toEqual([])
  })

  it('stops a destination that never answers once destinationTimeoutMs passes, and waits without a limit at zero', async () => {
    const db = await workspace.open('deadline')
    const labels = await seedOrders(db, 'order', 200, ORDER_PADDING)
    const destination = workspace.destination()
    destination.faults.holdWrite = (name, index) =>
      name === 'hung.db' && index === 1 ? new Promise<void>(() => {}) : undefined

    const startedAt = Date.now()
    const hung = await failureOf(
      db.backupTo({ destination, name: 'hung.db', pieceBytes: PIECE_BYTES, destinationTimeoutMs: 150 }),
    )
    const waited = Date.now() - startedAt

    expect(codeOf(hung)).toBe('BACKUP_DESTINATION_ERROR')
    expect(hung.message).toBe("The destination did not store piece 1 of 'hung.db' within 150ms, so the run stopped")
    expect(waited).toBeLessThan(5_000)

    destination.faults.holdWrite = (name, index) =>
      name.startsWith('slow') && index === 0
        ? new Promise<void>(settle => setTimeout(settle, SLOW_STORE_MS))
        : undefined
    const tooShort = await failureOf(
      db.backupTo({ destination, name: 'slow-bounded.db', pieceBytes: PIECE_BYTES, destinationTimeoutMs: 100 }),
    )
    const unbounded = await db.backupTo({ destination, name: 'slow-unbounded.db', destinationTimeoutMs: 0 })
    const atTheLongestTimer = await db.backupTo({
      destination,
      name: 'slow-longest.db',
      destinationTimeoutMs: LONGEST_TIMER_MS,
    })

    expect(codeOf(tooShort)).toBe('BACKUP_DESTINATION_ERROR')
    for (const name of [unbounded.destinationName, atTheLongestTimer.destinationName]) {
      expect(orderLabels(rebuilt(`${name}-joined.db`, joinPieces(destination, name, 16 * 1024 * 1024)))).toEqual(labels)
    }
  })

  it('stages the copy in a directory it creates, and refuses a staging path that is a file with BACKUP_ERROR', async () => {
    const db = await workspace.open('staging-paths')
    const labels = await seedOrders(db, 'order', 30)
    const destination = workspace.destination()
    const missing = workspace.path('not/yet/made')
    const file = workspace.path('a-file')
    writeFileSync(file, 'not a directory')

    const report = await db.backupTo({ destination, name: 'staged.db', stagingDir: missing })
    const refused = await failureOf(db.backupTo({ destination, name: 'refused.db', stagingDir: file }))

    expect(orderLabels(rebuilt('staged-joined.db', joinPieces(destination, 'staged.db', report.pieceBytes)))).toEqual(
      labels,
    )
    expect(filesIn(missing)).toEqual([])
    expect(codeOf(refused)).toBe('BACKUP_ERROR')
    expect(refused.message).toContain(file)
    expect(destination.names()).toEqual(['staged.db'])
    expect(readFileSync(file, 'utf8')).toBe('not a directory')
  })

  it('refuses a destination deadline that is negative, not a number, or longer than a timer holds', async () => {
    const db = await workspace.open('bad-deadlines')
    await seedOrders(db, 'order', 3)
    const destination = workspace.destination()

    for (const destinationTimeoutMs of [-1, Number.NaN, Number.POSITIVE_INFINITY, LONGEST_TIMER_MS + 1]) {
      const error = await failureOf(db.backupTo({ destination, name: 'never.db', destinationTimeoutMs }))
      expect(codeOf(error), `${destinationTimeoutMs}`).toBe('BACKUP_ERROR')
      expect(error.message, `${destinationTimeoutMs}`).toContain(`${destinationTimeoutMs}`)
    }
    expect(destination.names()).toEqual([])
  })

  it('refuses to assemble a copy whose pieces are missing, stale, short, or altered, and leaves no partial file', async () => {
    const db = await workspace.open('assembled')
    const labels = await seedOrders(db, 'order', 400, ORDER_PADDING)
    const destination = workspace.destination()
    const report = await db.backupTo({ destination, name: 'kept.db', pieceBytes: PIECE_BYTES })
    const lastIndex = report.pieceCount - 1
    const original = (index: number) => new Uint8Array(destination.stored('kept.db', index))

    const clean = await assembleFromDestination(destination, report, workspace.path('clean.db'))
    expect(clean).toEqual({
      bytesWritten: report.bytesWritten,
      pieceCount: report.pieceCount,
      fingerprint: sha256(joinPieces(destination, 'kept.db', PIECE_BYTES)),
    })
    expect(orderLabels(workspace.path('clean.db'))).toEqual(labels)

    const occupied = workspace.path('occupied.db')
    writeFileSync(occupied, 'a database the operator still needs')
    const piece1 = original(1)
    destination.remove('kept.db', 1)
    const missing = await failureOf(assembleFromDestination(destination, report, occupied))
    destination.replace('kept.db', 1, piece1)
    expect(codeOf(missing)).toBe('BACKUP_DESTINATION_ERROR')
    expect(missing.message).toContain("missing piece 1 of 'kept.db'")
    expect(readFileSync(occupied, 'utf8')).toBe('a database the operator still needs')

    const tampering: [string, () => () => void, string][] = [
      [
        'a stale piece past the end',
        () => {
          destination.replace('kept.db', lastIndex + 1, new Uint8Array(PIECE_BYTES))
          return () => destination.remove('kept.db', lastIndex + 1)
        },
        'belongs to a different run',
      ],
      [
        'a short last piece',
        () => {
          const last = original(lastIndex)
          destination.replace('kept.db', lastIndex, last.subarray(0, last.byteLength - 1))
          return () => destination.replace('kept.db', lastIndex, last)
        },
        `hold ${report.bytesWritten - 1} bytes where the run wrote ${report.bytesWritten}`,
      ],
      [
        'a flipped byte',
        () => {
          const first = original(0)
          const altered = first.slice()
          altered[100] = (altered[100] ?? 0) ^ 0xff
          destination.replace('kept.db', 0, altered)
          return () => destination.replace('kept.db', 0, first)
        },
        'do not match the fingerprint',
      ],
    ]
    for (const [label, tamper, message] of tampering) {
      const restore = tamper()
      const target = workspace.path(`${label.replaceAll(' ', '-')}.db`)
      const error = await failureOf(assembleFromDestination(destination, report, target))
      restore()
      expect(codeOf(error), label).toBe('BACKUP_DESTINATION_ERROR')
      expect(error.message, label).toContain(message)
      expect(filesIn(workspace.dir).includes(`${label.replaceAll(' ', '-')}.db`), label).toBe(false)
    }
  })
})

describe('a copy that SQLite restarts or that stops moving', () => {
  it('counts each restart another connection causes and still sends a whole copy of the final state', async () => {
    const sourcePath = workspace.path('restarted.db')
    const db = await workspace.open('restarted')
    await seedOrders(db, 'seeded', 20_000, ORDER_PADDING)
    const intruder = new BetterSqlite3(sourcePath)
    intruder.pragma('busy_timeout = 5000')
    const insert = intruder.prepare(INSERT_ORDER)
    const intrusionsWanted = 3
    let intrusions = 0
    let restartsAtLastIntrusion = -1
    const destination = workspace.destination()

    const report = await db.backupTo({
      destination,
      name: 'restarted.db',
      pieceBytes: 65_536,
      pagesPerStep: 8,
      restartLimit: 100,
      noProgressStepLimit: 10_000,
      onProgress: progress => {
        const copied = progress.totalPages - progress.remainingPages
        if (progress.phase !== 'copy' || intrusions >= intrusionsWanted || copied < 80) return
        if (progress.restarts === restartsAtLastIntrusion) return
        restartsAtLastIntrusion = progress.restarts
        insert.run(`intruder-${intrusions}`, '')
        intrusions++
      },
    })
    intruder.close()

    const copy = rebuilt('restarted-joined.db', joinPieces(destination, 'restarted.db', 65_536))
    expect(intrusions).toBe(intrusionsWanted)
    expect(report.restarts).toBe(intrusionsWanted)
    expect(integrityOf(copy)).toBe('ok')
    expect(orderLabels(copy).slice(20_000)).toEqual(['intruder-0', 'intruder-1', 'intruder-2'])
  })

  it('gives up with BACKUP_RESTARTED one restart past restartLimit, and after noProgressStepLimit steps that gain nothing', async () => {
    const sourcePath = workspace.path('contended.db')
    const db = await workspace.open('contended')
    await seedOrders(db, 'seeded', 20_000, ORDER_PADDING)
    const intruder = new BetterSqlite3(sourcePath)
    intruder.pragma('busy_timeout = 5000')
    const insert = intruder.prepare(INSERT_ORDER)
    let intrusions = 0
    const intrudeOnEveryStep = (progress: BackupProgress) => {
      if (progress.phase !== 'copy' || progress.remainingPages === 0) return
      insert.run(`intruder-${intrusions}`, '')
      intrusions++
    }
    const stagingDir = workspace.path('staging')
    const destination = workspace.destination()

    try {
      const pastTheLimit = await failureOf(
        db.backupTo({
          destination,
          name: 'restarted.db',
          pagesPerStep: 64,
          restartLimit: 2,
          noProgressStepLimit: 10_000,
          stagingDir,
          onProgress: progress => {
            if (progress.totalPages - progress.remainingPages >= 256) intrudeOnEveryStep(progress)
          },
        }),
      )
      const noGround = await failureOf(
        db.backupTo({
          destination,
          name: 'no-ground.db',
          pagesPerStep: 64,
          restartLimit: 10_000,
          noProgressStepLimit: 5,
          stagingDir,
          onProgress: intrudeOnEveryStep,
        }),
      )

      expect(codeOf(pastTheLimit)).toBe('BACKUP_RESTARTED')
      expect(pastTheLimit.message).toContain('returned to page one 3 times')
      expect(codeOf(noGround)).toBe('BACKUP_RESTARTED')
      expect(noGround.message).toContain('moved no page it had not already moved across 6 steps')
      expect(destination.names()).toEqual([])
      expect(filesIn(stagingDir)).toEqual([])
    } finally {
      intruder.close()
    }
  })

  it('reports a copy that moves no pages as BACKUP_STALLED without waiting for it, and removes its staged file once it stops', async () => {
    const watch = watchUnhandledRejections()
    try {
      const sourcePath = workspace.path('stalling.db')
      const db = await workspace.registry(stallingCopyDriver()).open('stalling', sourcePath, { writerWorker: true })
      await seedOrders(db, 'order', 50)
      const stagingDir = workspace.path('staging')
      mkdirSync(stagingDir)
      const destination = workspace.destination()

      const startedAt = Date.now()
      const error = await failureOf(db.backupTo({ destination, name: 'stalled.db', stagingDir, stallTimeoutMs: 300 }))
      const waited = Date.now() - startedAt

      expect(codeOf(error)).toBe('BACKUP_STALLED')
      expect(error.message).toContain('moved no pages for 300ms')
      expect(waited).toBeLessThan(COPY_STALL_MS)
      expect(filesIn(stagingDir)).toHaveLength(1)
      expect(destination.names()).toEqual([])

      await waitFor(() => filesIn(stagingDir).length === 0, 'the stalled copy to stop', COPY_STALL_MS * 3)
      await db.execute(INSERT_ORDER, ['after-the-stall', ''])
      const later = await db.backupTo({ destination, name: 'later.db', stallTimeoutMs: COPY_STALL_MS * 5 })
      expect(
        orderLabels(rebuilt('later-joined.db', joinPieces(destination, 'later.db', later.pieceBytes))).at(-1),
      ).toBe('after-the-stall')
      expect(readRaw(sourcePath, 'SELECT count(*) AS n FROM orders')).toEqual([{ n: 51 }])
      expect(watch.reasons).toEqual([])
    } finally {
      watch.stop()
    }
  }, 30_000)
})
