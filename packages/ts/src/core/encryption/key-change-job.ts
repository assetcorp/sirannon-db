import { createMainSchemaTriggers, dropMainSchemaTriggers } from '../cdc/trigger-sql.js'
import type { ConnectionPool } from '../connection-pool.js'
import type { DatabaseBackupController } from '../database-backup.js'
import type { DatabaseCdcController } from '../database-cdc.js'
import type { DatabaseFileSystem } from '../driver/file-system.js'
import type { SQLiteConnection, SQLiteDriver } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import type { DatabaseFileConnections } from '../file-connections.js'
import type { WriterLock } from '../writer-lock.js'
import { EncryptionError } from './errors.js'
import { type EncryptionKeySource, resolveEncryptionKey, wipeEncryptionKey } from './key.js'
import { startKeyChangeCapture } from './key-change-capture.js'
import {
  attachNewFile,
  attachOriginal,
  copyUncapturedTables,
  detachNewFile,
  detachOriginal,
  exportSnapshot,
  readSchemaVersion,
} from './key-change-copy.js'
import { FILE_IDENTITY_BYTES, keyChangeFilePath, removeKeyChangeFile } from './key-change-files.js'
import { createKeyChangeMarker, type KeyChangeMarker, removeKeyChangeMarker } from './key-change-marker.js'
import { type KeyChangeCapture, keyChangeAborted } from './key-change-round.js'
import { writeKeyChange } from './key-metadata.js'
import { EncryptionKeyring } from './keyring.js'
import type { EncryptionChangeStatus } from './types.js'

const MAX_CATCH_UP_ROUNDS = 16
const READ_CONNECTION_DRAIN_TIMEOUT_MS = 30_000

/**
 * The parts of an open database that a key change works through.
 *
 * @internal
 */
export interface KeyChangeDeps {
  path: string
  driver: SQLiteDriver
  keyring: EncryptionKeyring
  pool: ConnectionPool
  writerLock: WriterLock
  fileConnections: DatabaseFileConnections
  cdc: DatabaseCdcController
  backups: DatabaseBackupController
}

/**
 * Receives the progress of a key change, and tells the change whether the database has started to close.
 *
 * @internal
 */
export interface KeyChangeProgress {
  update(fields: Partial<EncryptionChangeStatus>): void
  readonly cancelled: boolean
}

async function reopenPool(pool: ConnectionPool, cdc: DatabaseCdcController): Promise<void> {
  await pool.reopen()
  cdc.resumeOnReopenedPool()
}

async function assertSchemaUnchanged(writer: SQLiteConnection, expected: bigint): Promise<void> {
  if ((await readSchemaVersion(writer)) !== expected) {
    throw keyChangeAborted(
      'The schema changed while the key change copied the database, so the new file would miss that change',
    )
  }
}

async function checkpointTruncate(writer: SQLiteConnection): Promise<void> {
  const row = (await (await writer.prepare('PRAGMA wal_checkpoint(TRUNCATE)')).get()) as
    | Record<string, unknown>
    | undefined
  const busy = row === undefined ? 0 : Number(Object.values(row)[0])
  if (busy !== 0) {
    throw keyChangeAborted('Another connection held the write-ahead log open, so the key change could not empty it')
  }
}

function requireFiles(driver: SQLiteDriver, path: string): DatabaseFileSystem {
  if (driver.files === undefined || path === ':memory:' || path === '' || path.startsWith('file:')) {
    throw new EncryptionError(
      'A key change needs a database file on a driver that supplies file operations',
      'ENCRYPTION_UNSUPPORTED',
    )
  }
  return driver.files
}

/**
 * Copies a database to a new file under a target key, applies the writes committed since the copy, and replaces the
 * original file with the new one, keeping reads and writes going until the last round.
 *
 * @internal
 */
export class KeyChangeRun {
  private readonly files: DatabaseFileSystem
  private readonly newPath: string
  private readonly targetRing: EncryptionKeyring
  private snapshot: SQLiteConnection | null = null
  private targetConn: SQLiteConnection | null = null
  private capture: KeyChangeCapture | null = null
  private marker: KeyChangeMarker | null = null
  private triggers: string[] = []
  private schemaVersion = 0n
  private rounds = 0
  private connectionsClosed = false
  private cyclePaused = false
  private copiesPaused = false
  private chainForgotten = false
  private replaced = false
  private stranded = false

  /**
   * Checks that the database can change its key, without touching any file.
   *
   * @param deps - The parts of the open database.
   * @param target - The key function of the target key, or `null` for a plaintext file.
   * @param progress - Receives the progress, and reports a close of the database.
   * @throws An `EncryptionError` with `ENCRYPTION_UNSUPPORTED` when the driver or the path cannot hold a key change.
   */
  constructor(
    private readonly deps: KeyChangeDeps,
    private readonly target: EncryptionKeySource | null,
    private readonly progress: KeyChangeProgress,
  ) {
    this.files = requireFiles(deps.driver, deps.path)
    this.newPath = keyChangeFilePath(deps.path)
    this.targetRing = new EncryptionKeyring(target)
  }

  /**
   * Runs the key change to the end.
   *
   * @throws An `EncryptionError` with `ENCRYPTION_CHANGE_ABORTED` when the change stops, which leaves the database
   * under the key that it had, or the error of the target key function.
   */
  async run(): Promise<void> {
    try {
      await this.copySnapshot()
      await this.markNewFile()
      await this.catchUp()
      await this.replaceFile()
    } catch (err) {
      await this.cleanUp()
      if (err instanceof SirannonError) throw err
      if (this.stranded) {
        throw keyChangeAborted(
          'The key change stopped while it moved the new file into place, so the database stays closed until the next open finishes the move',
          err,
        )
      }
      throw keyChangeAborted('The key change stopped, and the database keeps the key that it had', err)
    }
  }

  private checkOpen(): void {
    if (this.progress.cancelled) throw keyChangeAborted('The database closed while the key change ran')
  }

  private requireCapture(): KeyChangeCapture {
    if (this.capture === null)
      throw keyChangeAborted('The key change lost its record of the writes made since the copy')
    return this.capture
  }

  private requireTarget(): SQLiteConnection {
    if (this.targetConn === null) throw keyChangeAborted('The key change lost its connection to the new file')
    return this.targetConn
  }

  private requireMarker(): KeyChangeMarker {
    if (this.marker === null) throw keyChangeAborted('The key change lost its marker')
    return this.marker
  }

  private async copySnapshot(): Promise<void> {
    const { pool, writerLock, fileConnections, path } = this.deps
    await removeKeyChangeFile(this.files, path)
    await removeKeyChangeMarker(this.files, path)
    const snapshot = await fileConnections.open({ walMode: false, walAutoCheckpoint: 0 })
    this.snapshot = snapshot
    const targetKey = this.target === null ? undefined : await resolveEncryptionKey(this.target)
    try {
      await attachNewFile(snapshot, this.newPath, targetKey?.bytes)
    } finally {
      wipeEncryptionKey(targetKey)
    }
    this.schemaVersion = await writerLock.run(async () => {
      this.capture = await startKeyChangeCapture(pool.acquireWriter())
      await snapshot.exec('BEGIN')
      await (await snapshot.prepare('SELECT count(*) FROM sqlite_schema')).get()
      return readSchemaVersion(snapshot)
    })
    const rowsCopied = await exportSnapshot(snapshot)
    await snapshot.exec('COMMIT')
    await detachNewFile(snapshot)
    this.snapshot = null
    await snapshot.close()
    this.progress.update({ rowsCopied })
    this.checkOpen()
  }

  private async markNewFile(): Promise<void> {
    const { driver, path, keyring } = this.deps
    const identity = await this.files.readStart(this.newPath, FILE_IDENTITY_BYTES)
    if (identity === null || identity.length !== FILE_IDENTITY_BYTES) {
      throw keyChangeAborted('The key change wrote a new file too short to hold a database')
    }
    this.marker = await createKeyChangeMarker(driver, path, identity)
    this.targetConn = await this.targetRing.openWith(keyed =>
      driver.open(this.newPath, { walMode: false, synchronous: 'full', ...keyed }),
    )
    await this.targetConn.exec('PRAGMA foreign_keys = OFF')
    this.triggers = await dropMainSchemaTriggers(this.targetConn)
    const source = keyring.keySource
    const originalKey = source === null ? undefined : await resolveEncryptionKey(source)
    try {
      await attachOriginal(this.targetConn, path, originalKey?.bytes)
    } finally {
      wipeEncryptionKey(originalKey)
    }
  }

  private async catchUp(): Promise<void> {
    const { pool, writerLock } = this.deps
    this.progress.update({ state: 'catching-up', rounds: 0 })
    for (let round = 0; round < MAX_CATCH_UP_ROUNDS; round++) {
      this.checkOpen()
      const drained = await writerLock.run(async () => {
        const writer = pool.acquireWriter()
        await assertSchemaUnchanged(writer, this.schemaVersion)
        return this.requireCapture().drain(writer)
      })
      await drained.apply(this.requireTarget())
      this.rounds++
      this.progress.update({ rounds: this.rounds })
      if (drained.small) return
    }
  }

  private async replaceFile(): Promise<void> {
    const { backups, fileConnections, writerLock } = this.deps
    this.checkOpen()
    this.progress.update({ state: 'swapping' })
    this.cyclePaused = await backups.pauseCycle()
    this.copiesPaused = true
    await backups.pauseCopies()
    this.connectionsClosed = true
    await fileConnections.closeForReplacement(READ_CONNECTION_DRAIN_TIMEOUT_MS)
    this.checkOpen()
    await writerLock.run(() => this.finishWithWritesHeld())
    this.connectionsClosed = false
    await fileConnections.reopenAfterReplacement()
    this.copiesPaused = false
    backups.resumeCopies()
    if (this.cyclePaused) await backups.resumeCycle({ newChain: true })
  }

  private async finishNewFile(writer: SQLiteConnection): Promise<void> {
    const target = this.requireTarget()
    const capture = this.requireCapture()
    await assertSchemaUnchanged(writer, this.schemaVersion)
    await (await capture.drain(writer)).apply(target)
    this.rounds++
    this.progress.update({ rounds: this.rounds })
    await capture.finish(target)
    await copyUncapturedTables(target)
    await createMainSchemaTriggers(target, this.triggers)
    await detachOriginal(target)
    await writeKeyChange(target, this.targetRing.keyId, Date.now())
    this.targetConn = null
    await target.close()
    await capture.stop()
    this.capture = null
  }

  private async finishWithWritesHeld(): Promise<void> {
    const { pool, path, keyring, cdc } = this.deps
    const writer = pool.acquireWriter()
    await this.finishNewFile(writer)
    const marker = this.requireMarker()
    await pool.closeReaders(READ_CONNECTION_DRAIN_TIMEOUT_MS)
    if (this.cyclePaused) {
      await this.deps.backups.forgetPausedChain()
      this.chainForgotten = true
    }
    await marker.recordReplacementBegun()
    try {
      await checkpointTruncate(writer)
      await pool.closeWriter()
      await this.files.remove(`${path}-wal`)
      await this.files.remove(`${path}-shm`)
      await this.files.rename(this.newPath, path)
      this.replaced = true
      await marker.flushDirectory()
    } catch (err) {
      await this.recoverFromFailedSwap()
      throw err
    }
    keyring.replace(this.target)
    await reopenPool(pool, cdc)
    this.marker = null
    await marker.close()
    await removeKeyChangeMarker(this.files, path)
  }

  private async recoverFromFailedSwap(): Promise<void> {
    const { pool, path, keyring, cdc } = this.deps
    if (!this.replaced && (await this.files.readStart(path, 1)) === null) {
      try {
        await this.files.rename(this.newPath, path)
        this.replaced = true
      } catch {
        this.stranded = true
        return
      }
    }
    if (this.replaced) keyring.replace(this.target)
    await reopenPool(pool, cdc)
  }

  private async cleanUp(): Promise<void> {
    const { pool, writerLock, fileConnections, backups, keyring, path, cdc } = this.deps
    const snapshot = this.snapshot
    if (snapshot !== null) {
      await snapshot.exec('ROLLBACK').catch(() => undefined)
      await detachNewFile(snapshot).catch(() => undefined)
      await snapshot.close().catch(() => undefined)
    }
    await this.targetConn?.close().catch(() => undefined)
    const capture = this.capture
    if (capture !== null) await writerLock.run(() => capture.stop()).catch(() => undefined)
    await this.marker?.close().catch(() => undefined)
    this.marker = null
    if (this.stranded) {
      if (this.copiesPaused) backups.resumeCopies()
      return
    }
    if (this.replaced) keyring.replace(this.target)
    await writerLock
      .run(async () => {
        if (!pool.suspended) return
        await reopenPool(pool, cdc)
      })
      .catch(() => undefined)
    if (!this.replaced) {
      await removeKeyChangeFile(this.files, path).catch(() => undefined)
      await removeKeyChangeMarker(this.files, path).catch(() => undefined)
    }
    if (this.connectionsClosed) await fileConnections.reopenAfterReplacement()
    if (this.copiesPaused) backups.resumeCopies()
    if (this.cyclePaused) {
      await backups.resumeCycle({ newChain: this.replaced || this.chainForgotten }).catch(() => undefined)
    }
  }
}
