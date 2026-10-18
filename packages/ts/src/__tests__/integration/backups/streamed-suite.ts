import { readdirSync, writeFileSync } from 'node:fs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { BackupProgress } from '../../../core/backup/report.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { memoryDestination } from '../../helpers/memory-destination.js'
import { builtStreamingExtensionPath, driverStreamsToDestination } from '../../helpers/streaming-extension.js'
import {
  codeOf,
  createWorkspace,
  failureOf,
  filesIn,
  INSERT_ORDER,
  integrityOf,
  joinPieces,
  ORDER_PADDING,
  orderLabels,
  seedOrders,
  sha256,
  type Workspace,
} from './harness.js'

const PIECE_BYTES = 65_536
const SEEDED_ORDERS = 4_000
const COPIES_IN_ONE_PROCESS = 60
const SLOWEST_ACCEPTABLE_COPY_MS = 15_000
const REPEATED_COPIES_TIMEOUT_MS = 120_000
const SLOW_STORE_MS = 15
const COARSE_TIMER_TICK_MS = 1

export interface StreamingExtensionOptions {
  vfsExtensionPath?: string
}

export type DriverBuilder = (options: StreamingExtensionOptions) => SQLiteDriver

function streamingDriver(build: DriverBuilder): { extensionPath: string | null; driver: SQLiteDriver } {
  const extensionPath = builtStreamingExtensionPath()
  return { extensionPath, driver: build(extensionPath ? { vfsExtensionPath: extensionPath } : {}) }
}

function strayStreamFiles(): string[] {
  return readdirSync(process.cwd()).filter(name => name.startsWith('file:sirannon-stream'))
}

export function describeStreamedCopies(label: string, build: DriverBuilder, mustStream: boolean): void {
  const { extensionPath, driver } = streamingDriver(build)
  const streams = driverStreamsToDestination(driver)
  let workspace: Workspace

  beforeEach(() => {
    workspace = createWorkspace('streamed')
  })

  afterEach(async () => {
    await workspace.dispose()
  })

  describe(`streaming a copy to a destination with ${label}`, () => {
    it.skipIf(process.env.SIRANNON_REQUIRE_STREAMING_EXTENSION !== '1')(
      'finds the compiled extension and takes the route this runner requires',
      async () => {
        expect(extensionPath).not.toBeNull()
        expect(streams).toBe(mustStream)
        const db = await workspace.open('route', {}, driver)
        expect(db.backupCapabilities()).toMatchObject({
          streamedCopy: mustStream,
          localDiskRequired: mustStream ? 'none' : 'equal-to-backup',
        })
      },
    )

    describe.each([false, true])('with the writer worker set to %s', writerWorker => {
      it.skipIf(!streams)(
        'sends a whole copy while writes go on, writes no local file, and reports each piece as it travels',
        async () => {
          const sourcePath = workspace.path('orders.db')
          const db = await workspace.registry(driver).open('orders', sourcePath, { writerWorker })
          await seedOrders(db, 'seeded', SEEDED_ORDERS, ORDER_PADDING)
          await db.execute(INSERT_ORDER, ['written-after-the-load', ''])
          const destination = workspace.destination()
          const before = filesIn(workspace.dir)
          const seen: BackupProgress[] = []
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

          const report = await db.backupTo({
            destination,
            name: 'orders-copy.db',
            pieceBytes: PIECE_BYTES,
            pagesPerStep: 8,
            onProgress: progress => seen.push({ ...progress }),
          })
          copying = false
          await writer
          const after = filesIn(workspace.dir)

          const joined = joinPieces(destination, 'orders-copy.db', PIECE_BYTES)
          const copyPath = workspace.path('joined.db')
          writeFileSync(copyPath, joined)
          const labels = orderLabels(copyPath)
          expect(report).toMatchObject({ route: 'streamed', kind: 'full', restarts: 0, pieceBytes: PIECE_BYTES })
          expect(report.pieceCount).toBeGreaterThan(1)
          expect(destination.names()).toEqual(['orders-copy.db'])
          expect(destination.indices('orders-copy.db')).toEqual(
            Array.from({ length: report.pieceCount }, (_, at) => at),
          )
          expect(report.bytesWritten).toBe(joined.byteLength)
          expect(report.bytesWritten).toBe(report.pageCount * report.pageSize)
          expect(report.fingerprint).toBe(sha256(joined))
          expect(report.logPosition?.lastFrame).toBeGreaterThan(0)
          expect(after).toEqual(before)
          expect(strayStreamFiles()).toEqual([])
          expect(integrityOf(copyPath)).toBe('ok')
          expect(labels.slice(0, SEEDED_ORDERS + 1).at(-1)).toBe('written-after-the-load')
          expect(labels.slice(SEEDED_ORDERS + 1)).toEqual(
            Array.from({ length: labels.length - SEEDED_ORDERS - 1 }, (_, at) => `during-${at}`),
          )
          const transfers = seen.filter(progress => progress.phase === 'transfer')
          expect(seen.some(progress => progress.phase === 'copy')).toBe(true)
          expect(transfers).toHaveLength(report.pieceCount)
          expect(transfers.at(-1)?.bytesWritten).toBe(report.bytesWritten)
          expect(seen.every(progress => progress.runId === report.runId)).toBe(true)
        },
        30_000,
      )
    })

    it.skipIf(!streams)(
      'refuses a piece size that is no whole number of 512-byte blocks, and sends one of exactly 512',
      async () => {
        const db = await workspace.open('blocks', {}, driver)
        const labels = await seedOrders(db, 'order', 20)
        const destination = workspace.destination()

        for (const pieceBytes of [5_000, 511, 513, 1_000]) {
          const error = await failureOf(db.backupTo({ destination, name: 'refused.db', pieceBytes }))
          expect(codeOf(error), `${pieceBytes}`).toBe('BACKUP_ERROR')
          expect(error.message, `${pieceBytes}`).toContain('must divide by 512')
        }
        const report = await db.backupTo({ destination, name: 'blocks.db', pieceBytes: 512 })

        const copyPath = workspace.path('blocks-joined.db')
        writeFileSync(copyPath, joinPieces(destination, 'blocks.db', 512))
        expect(destination.names()).toEqual(['blocks.db'])
        expect(report.pieceCount).toBe(report.bytesWritten / 512)
        expect(orderLabels(copyPath)).toEqual(labels)
      },
    )

    it.skipIf(!streams)(
      'names the piece the destination refused, stops a destination that hangs, and streams the next copy whole',
      async () => {
        const db = await workspace.open('faults', {}, driver)
        const labels = await seedOrders(db, 'order', SEEDED_ORDERS, ORDER_PADDING)
        const destination = workspace.destination()
        destination.faults.refuseWrite = (name, index) => name === 'refused.db' && index === 1
        destination.faults.holdWrite = (name, index) =>
          name === 'hung.db' && index === 2 ? new Promise<void>(() => {}) : undefined

        const refused = await failureOf(db.backupTo({ destination, name: 'refused.db', pieceBytes: PIECE_BYTES }))
        const hung = await failureOf(
          db.backupTo({ destination, name: 'hung.db', pieceBytes: PIECE_BYTES, destinationTimeoutMs: 200 }),
        )
        const report = await db.backupTo({ destination, name: 'whole.db', pieceBytes: PIECE_BYTES })

        expect(codeOf(refused)).toBe('BACKUP_DESTINATION_ERROR')
        expect(refused.message).toContain("The destination refused piece 1 of 'refused.db'")
        expect(codeOf(hung)).toBe('BACKUP_DESTINATION_ERROR')
        expect(hung.message).toContain("did not store piece 2 of 'hung.db' within 200ms")
        const copyPath = workspace.path('whole-joined.db')
        writeFileSync(copyPath, joinPieces(destination, 'whole.db', PIECE_BYTES))
        expect(report.fingerprint).toBe(sha256(joinPieces(destination, 'whole.db', PIECE_BYTES)))
        expect(orderLabels(copyPath)).toEqual(labels)
      },
      30_000,
    )

    it.skipIf(!streams)('reads nothing back from the destination when the fingerprint is off', async () => {
      const db = await workspace.open('unprinted', {}, driver)
      const labels = await seedOrders(db, 'order', 500, ORDER_PADDING)
      const destination = workspace.destination()
      destination.faults.refuseRead = () => true

      const report = await db.backupTo({ destination, name: 'unprinted.db', pieceBytes: 8192, fingerprint: false })

      const copyPath = workspace.path('unprinted-joined.db')
      writeFileSync(copyPath, joinPieces(destination, 'unprinted.db', 8192))
      expect(report.fingerprint).toBeUndefined()
      expect(orderLabels(copyPath)).toEqual(labels)
    })

    it.skipIf(!streams)(
      'keeps streaming after the database that streamed first closes, and finishes every copy of many in one process',
      async () => {
        const slowest: number[] = []
        for (let run = 0; run < COPIES_IN_ONE_PROCESS; run++) {
          const db = await workspace.open(`repeat-${run}`, {}, driver)
          await seedOrders(db, `run-${run}`, SEEDED_ORDERS, ORDER_PADDING)
          const destination = memoryDestination()
          const startedAt = Date.now()
          const report = await db.backupTo({ destination, name: 'repeat.db', pieceBytes: PIECE_BYTES, pagesPerStep: 8 })
          slowest.push(Date.now() - startedAt)
          await db.close()
          expect(sha256(destination.bytesFor('repeat.db')), `copy ${run}`).toBe(report.fingerprint)
        }

        expect(Math.max(...slowest)).toBeLessThan(SLOWEST_ACCEPTABLE_COPY_MS)
      },
      REPEATED_COPIES_TIMEOUT_MS,
    )
  })
}

export function describeStreamedBackpressure(label: string, build: DriverBuilder): void {
  const { driver } = streamingDriver(build)
  const streams = driverStreamsToDestination(driver)
  let workspace: Workspace

  beforeEach(() => {
    workspace = createWorkspace('backpressure')
  })

  afterEach(async () => {
    await workspace.dispose()
  })

  describe(`streaming a copy to a slow destination with ${label}`, () => {
    it.skipIf(!streams)('holds the copy back while the destination is still storing a piece', async () => {
      const db = await workspace.open('backpressure', {}, driver)
      const labels = await seedOrders(db, 'order', SEEDED_ORDERS, ORDER_PADDING)
      const destination = workspace.destination()
      destination.faults.holdWrite = () => new Promise<void>(settle => setTimeout(settle, SLOW_STORE_MS))

      const report = await db.backupTo({ destination, name: 'slow.db', pieceBytes: PIECE_BYTES, pagesPerStep: 8 })

      const copyPath = workspace.path('slow-joined.db')
      writeFileSync(copyPath, joinPieces(destination, 'slow.db', PIECE_BYTES))
      expect(report.pieceCount).toBeGreaterThan(4)
      expect(report.transferMs).toBeGreaterThanOrEqual(report.pieceCount * (SLOW_STORE_MS - COARSE_TIMER_TICK_MS))
      expect(report.copyMs).toBeGreaterThanOrEqual(report.transferMs / 2)
      expect(orderLabels(copyPath)).toEqual(labels)
    })
  })
}
