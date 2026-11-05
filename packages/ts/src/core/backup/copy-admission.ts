import type { DatabaseCopyRequest, DatabaseCopyStep, SQLiteConnection } from '../driver/types.js'

type RunExclusive = (op: () => Promise<void>) => Promise<void>

/**
 * Lets a key change stop new backup copies and wait for the ones that run, so that every copy reads one file under
 * one key from its first page to its last, and so that a copy that starts after the change reads the file that the
 * change left in place.
 *
 * @internal
 */
export class BackupCopyAdmission {
  private paused = false
  private running = 0
  private reopened: Promise<void> = Promise.resolve()
  private openAgain: () => void = () => {}
  private copiesStopped: (() => void) | null = null

  constructor(private readonly runExclusive: RunExclusive) {}

  /**
   * Runs an operation while no other operation holds the writer, after any key change that stopped copies has ended.
   *
   * @param op - The operation to run.
   */
  readonly runAdmitted: RunExclusive = async op => {
    const admission = { ran: false }
    while (!admission.ran) {
      await this.reopened
      await this.runExclusive(async () => {
        if (this.paused) return
        admission.ran = true
        await op()
      })
    }
  }

  /**
   * Returns a connection that calls the pool's writer of the moment on every call, and that counts each copy it
   * starts until SQLite stops writing that copy, including a copy that the caller stopped waiting for.
   *
   * @param acquireWriter - Returns the pool's writer.
   * @returns The connection for the backup engine to copy through.
   */
  writer(acquireWriter: () => SQLiteConnection): SQLiteConnection {
    const track = (copy: Promise<DatabaseCopyStep>) => this.track(copy)
    return {
      exec: sql => acquireWriter().exec(sql),
      prepare: sql => acquireWriter().prepare(sql),
      transaction: fn => acquireWriter().transaction(fn),
      close: () => acquireWriter().close(),
      get copyRunsOffCallerThread() {
        return acquireWriter().copyRunsOffCallerThread
      },
      get copyDatabase() {
        const writer = acquireWriter()
        const copy = writer.copyDatabase
        if (copy === undefined) return undefined
        return (request: DatabaseCopyRequest) => track(copy.call(writer, request))
      },
    }
  }

  /** Stops new copies from starting, then waits until SQLite has stopped writing every copy that started before. */
  async pause(): Promise<void> {
    await this.runExclusive(async () => {
      if (this.paused) return
      this.paused = true
      this.reopened = new Promise<void>(resolve => {
        this.openAgain = resolve
      })
    })
    while (this.running > 0) {
      await new Promise<void>(resolve => {
        this.copiesStopped = resolve
      })
    }
  }

  /** Lets copies start again, on the connections that the pool holds now. */
  resume(): void {
    if (!this.paused) return
    this.paused = false
    this.openAgain()
  }

  private track(copy: Promise<DatabaseCopyStep>): Promise<DatabaseCopyStep> {
    this.running++
    const stopped = () => {
      this.running--
      if (this.running > 0) return
      const copiesStopped = this.copiesStopped
      this.copiesStopped = null
      copiesStopped?.()
    }
    copy.then(stopped, stopped)
    return copy
  }
}
