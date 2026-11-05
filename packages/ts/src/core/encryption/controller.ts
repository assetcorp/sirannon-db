import type { SQLiteConnection } from '../driver/types.js'
import { ReadOnlyError, SirannonError } from '../errors.js'
import type { RegisteredReplicationEngines } from '../replication-engines.js'
import { EncryptionError } from './errors.js'
import type { EncryptionKeySource } from './key.js'
import { type KeyChangeDeps, type KeyChangeProgress, KeyChangeRun } from './key-change-job.js'
import { readKeyMetadata, type StoredKeyMetadata } from './key-metadata.js'
import type { EncryptionChange, EncryptionChangeStatus, EncryptionStatus } from './types.js'

/**
 * What the encryption controller needs beyond the parts that a key change works through.
 *
 * @internal
 */
export interface EncryptionControllerDeps extends KeyChangeDeps {
  databaseId: string
  readOnly: boolean
  replicationEngines: RegisteredReplicationEngines
  onKeyChanged?: (key: EncryptionKeySource | null) => void
}

interface CipherDescription {
  cipher?: 'aegis-256' | 'sqlcipher-4'
  cipherVersion?: string
}

interface FileDescription extends StoredKeyMetadata, CipherDescription {}

const AEGIS_CIPHER_NAME = 'aegis'
const AEGIS_256_ALGORITHM_NAME = 'aegis-256'

function changeInProgress(): EncryptionError {
  return new EncryptionError(
    'A key change is already running on this database; wait for it to end before you start another',
    'ENCRYPTION_CHANGE_IN_PROGRESS',
  )
}

function engineDuringChange(): EncryptionError {
  return new EncryptionError(
    'A key change is replacing the file of this database, so a replication engine would stay on the replaced file; start the engine once the change ends',
    'ENCRYPTION_CHANGE_IN_PROGRESS',
  )
}

function invalidChange(): EncryptionError {
  return new EncryptionError(
    'changeEncryption takes { key }, where key is a function that returns 32 key bytes, or null to remove encryption',
    'INVALID_ENCRYPTION_KEY',
  )
}

async function readText(conn: SQLiteConnection, sql: string): Promise<string | undefined> {
  const row = (await (await conn.prepare(sql)).get()) as Record<string, unknown> | undefined
  const value = row === undefined ? undefined : Object.values(row)[0]
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

async function readCipher(conn: SQLiteConnection): Promise<CipherDescription> {
  const hasEngineVersion = await readText(
    conn,
    "SELECT 'present' FROM pragma_function_list WHERE name = 'sqlite3mc_version'",
  )
  if (hasEngineVersion === undefined) {
    const sqlcipherVersion = await readText(conn, 'PRAGMA cipher_version')
    return sqlcipherVersion === undefined ? {} : { cipher: 'sqlcipher-4', cipherVersion: sqlcipherVersion }
  }
  const engineVersion = await readText(conn, 'SELECT sqlite3mc_version()')
  const cipher = await readText(conn, 'PRAGMA cipher')
  const algorithm = await readText(conn, 'PRAGMA algorithm')
  return {
    ...(cipher === AEGIS_CIPHER_NAME && algorithm === AEGIS_256_ALGORITHM_NAME ? { cipher: 'aegis-256' as const } : {}),
    ...(engineVersion === undefined ? {} : { cipherVersion: engineVersion }),
  }
}

/**
 * Reports how a database is encrypted, and runs one key change at a time.
 *
 * @internal
 */
export class DatabaseEncryptionController {
  private change: EncryptionChangeStatus = { state: 'idle' }
  private running: Promise<void> | null = null
  private closing = false
  private described: FileDescription | null = null

  constructor(private readonly deps: EncryptionControllerDeps) {}

  /** `true` while the database file opens under a key. */
  get encrypted(): boolean {
    return this.deps.keyring.encrypted
  }

  /**
   * Returns whether the file is encrypted, the scheme that it is encrypted under, the engine's version, the stored
   * key identifier and change time, and the current or last key change.
   *
   * @returns The status.
   */
  async status(): Promise<EncryptionStatus> {
    const encrypted = this.deps.keyring.encrypted
    if (this.described === null && !this.deps.pool.suspended) this.described = await this.describeFile(encrypted)
    const described = this.described ?? {}
    return {
      encrypted,
      ...(encrypted && described.cipher !== undefined ? { cipher: described.cipher } : {}),
      ...(encrypted && described.cipherVersion !== undefined ? { cipherVersion: described.cipherVersion } : {}),
      ...(encrypted && described.keyId !== undefined ? { keyId: described.keyId } : {}),
      ...(described.changedAt === undefined ? {} : { changedAt: described.changedAt }),
      change: { ...this.change },
    }
  }

  /**
   * Encrypts the database, changes its key, or removes encryption, and resolves once the change ends.
   *
   * @param change - The target key function, or `null` for a plaintext file.
   * @returns The status after the change.
   * @throws `READ_ONLY` on a read-only database, `ENCRYPTION_CHANGE_IN_PROGRESS` while another change runs,
   * `REPLICATION_ACTIVE` while a replication engine writes to the database, and the error that stopped the change.
   */
  async changeKey(change: EncryptionChange): Promise<EncryptionStatus> {
    if (this.deps.readOnly) throw new ReadOnlyError(this.deps.databaseId)
    const target = this.targetOf(change)
    if (this.running !== null) throw changeInProgress()
    if (!this.deps.driver.capabilities.encryption) {
      throw new EncryptionError(
        'The driver that this Sirannon uses cannot encrypt a database, so it cannot change its key',
        'ENCRYPTION_UNSUPPORTED',
      )
    }
    const endReplacement = this.deps.replicationEngines.beginReplacement('key change', engineDuringChange)
    try {
      return await this.runChange(target)
    } finally {
      endReplacement()
    }
  }

  private async runChange(target: EncryptionKeySource | null): Promise<EncryptionStatus> {
    const isClosing = () => this.closing
    const progress: KeyChangeProgress = {
      update: (fields: Partial<EncryptionChangeStatus>) => {
        this.change = { ...this.change, ...fields }
      },
      get cancelled() {
        return isClosing()
      },
    }
    const run = new KeyChangeRun(this.deps, target, progress)
    this.change = { state: 'copying', target: target === null ? 'plaintext' : 'encrypted', startedAt: Date.now() }
    const outcome = run.run().then(
      () => this.finished(target),
      (err: unknown) => this.failed(err),
    )
    this.running = outcome.then(() => undefined)
    try {
      const failure = await outcome
      if (failure !== null) throw failure
    } finally {
      this.running = null
    }
    return this.status()
  }

  /** Stops a running key change at its next step, and waits for it to leave the database as it was. */
  async cancel(): Promise<void> {
    this.closing = true
    await this.running
  }

  private targetOf(change: EncryptionChange): EncryptionKeySource | null {
    if (typeof change !== 'object' || change === null || !('key' in change)) throw invalidChange()
    if (change.key === null) return null
    if (typeof change.key !== 'function') throw invalidChange()
    return change.key
  }

  private finished(target: EncryptionKeySource | null): null {
    this.change = { ...this.change, state: 'done', finishedAt: Date.now() }
    this.described = null
    this.deps.onKeyChanged?.(target)
    return null
  }

  private failed(err: unknown): unknown {
    const code = err instanceof SirannonError ? err.code : 'ENCRYPTION_CHANGE_ABORTED'
    const message = err instanceof Error ? err.message : 'The key change stopped'
    this.change = { ...this.change, state: 'failed', finishedAt: Date.now(), error: { code, message } }
    return err
  }

  private async describeFile(encrypted: boolean): Promise<FileDescription> {
    const { pool, writerLock } = this.deps
    const read = async (conn: SQLiteConnection): Promise<FileDescription> => {
      const metadata = await readKeyMetadata(conn)
      return { ...metadata, ...(encrypted ? await readCipher(conn) : {}) }
    }
    if (pool.readerCount > 0) return pool.withReader(read)
    return writerLock.run(() => read(pool.acquireReader()))
  }
}
