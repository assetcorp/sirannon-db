import type { Database } from '../core/database.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import { SirannonError } from '../core/errors.js'
import { DEVICE_CURSORS_TABLE } from '../core/internal-tables.js'
import {
  ensureDeviceCursorsTable,
  prepareUpdateDeviceCursor,
  prepareUpsertDeviceCursor,
  selectTableExists,
} from '../core/system-catalog/index.js'

const ACKS_PER_TRANSACTION = 5_000
const ACK_FLUSH_SPACING_MS = 200

interface AckWaiter {
  resolve: (stored: boolean) => void
  reject: (error: unknown) => void
}

interface PendingAck {
  deviceId: string
  seq: bigint
  cursorHeld: boolean
  waiters: AckWaiter[]
}

/**
 * Stores the acknowledgements of the devices on one database, many in each write transaction.
 *
 * A separate transaction for each acknowledgement would queue one writer-lock hold per acknowledgement ahead of the
 * application's writes, which comes to thousands of holds a second when thousands of devices acknowledge every few
 * seconds, and each of those commits rewrites most of the cursor table's pages. The writer stores an acknowledgement
 * that arrives while it is idle at once, then starts the next transaction no sooner than 200 ms after the start of the
 * one before, and stores up to 5,000 acknowledgements in each, keeping the highest sequence when one device
 * acknowledges twice. A transaction that fails rejects every acknowledgement in it.
 *
 * @internal
 */
export class DeviceAckWriter {
  private pending = new Map<string, PendingAck>()
  private flushing = false
  private lastFlushStartedAt = Number.NEGATIVE_INFINITY

  /**
   * @param database - The database whose cursor table the writer updates.
   * @param onCursorsStored - Called after a transaction stores acknowledgements, so that the change feed can prune
   * changes that those cursors release and schedule cursor expiry.
   */
  constructor(
    private readonly database: Database,
    private readonly onCursorsStored: () => void,
  ) {}

  /**
   * Stores a device's acknowledged sequence in the next transaction.
   *
   * @param deviceId - The device that acknowledged.
   * @param seq - The highest sequence that the device has acknowledged.
   * @param cursorHeld - Whether the database held a cursor for the device when it subscribed, in which case the write
   * moves that cursor and fails when the server has deleted it, rather than creating a cursor.
   * @returns `true` once the transaction commits the cursor, and `false` when the server has deleted a held cursor.
   * @throws `DATABASE_CLOSED` when the database closes before the write, and the driver's error when the write fails.
   */
  record(deviceId: string, seq: bigint, cursorHeld: boolean): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      const key = `${cursorHeld ? 'held' : 'new'}:${deviceId}`
      let ack = this.pending.get(key)
      if (ack === undefined) {
        ack = { deviceId, seq, cursorHeld, waiters: [] }
        this.pending.set(key, ack)
      } else if (seq > ack.seq) {
        ack.seq = seq
      }
      ack.waiters.push({ resolve, reject })
      this.scheduleFlush()
    })
  }

  private scheduleFlush(): void {
    if (this.flushing) return
    this.flushing = true
    const delay = this.lastFlushStartedAt + ACK_FLUSH_SPACING_MS - performance.now()
    if (delay <= 0) {
      queueMicrotask(this.startFlush)
      return
    }
    const timer = setTimeout(this.startFlush, delay) as ReturnType<typeof setTimeout> & { unref?: () => void }
    timer.unref?.()
  }

  private readonly startFlush = (): void => {
    void this.flushPending()
  }

  private async flushPending(): Promise<void> {
    try {
      this.lastFlushStartedAt = performance.now()
      await this.store(this.takeBatch())
    } finally {
      this.flushing = false
      if (this.pending.size > 0) this.scheduleFlush()
    }
  }

  private takeBatch(): PendingAck[] {
    const batch: PendingAck[] = []
    for (const [key, ack] of this.pending) {
      if (batch.length >= ACKS_PER_TRANSACTION) break
      batch.push(ack)
      this.pending.delete(key)
    }
    return batch
  }

  private async store(batch: PendingAck[]): Promise<void> {
    const outcomes: { stored: boolean[] | null } = { stored: null }
    try {
      await this.database.runCdcMaintenance(async writer => {
        outcomes.stored = await writer.transaction(tx => writeAcks(tx, batch))
      })
    } catch (err) {
      settleBatch(batch, waiter => waiter.reject(err))
      return
    }
    const stored = outcomes.stored
    if (stored === null) {
      const closed = new SirannonError(`Database '${this.database.id}' is closed`, 'DATABASE_CLOSED')
      settleBatch(batch, waiter => waiter.reject(closed))
      return
    }
    batch.forEach((ack, index) => {
      const outcome = stored[index] === true
      for (const waiter of ack.waiters) waiter.resolve(outcome)
    })
    if (stored.some(outcome => outcome)) this.onCursorsStored()
  }
}

function settleBatch(batch: readonly PendingAck[], settle: (waiter: AckWaiter) => void): void {
  for (const ack of batch) {
    for (const waiter of ack.waiters) settle(waiter)
  }
}

async function writeAcks(tx: SQLiteConnection, batch: readonly PendingAck[]): Promise<boolean[]> {
  const updatedAt = Date.now() / 1000
  const createsCursor = batch.some(ack => !ack.cursorHeld)
  let tableExists = await selectTableExists(tx, DEVICE_CURSORS_TABLE)
  if (!tableExists && createsCursor) {
    await ensureDeviceCursorsTable(tx)
    tableExists = true
  }
  const upsert = createsCursor ? await prepareUpsertDeviceCursor(tx) : null
  const update = tableExists && batch.some(ack => ack.cursorHeld) ? await prepareUpdateDeviceCursor(tx) : null
  const outcomes: boolean[] = []
  for (const ack of batch) {
    if (!ack.cursorHeld && upsert !== null) {
      await upsert(ack.deviceId, ack.seq, updatedAt)
      outcomes.push(true)
      continue
    }
    outcomes.push(update !== null && (await update(ack.deviceId, ack.seq, updatedAt)) > 0)
  }
  return outcomes
}
