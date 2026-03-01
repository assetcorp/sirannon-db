import { Database } from './database.js'
import { DatabaseAlreadyExistsError, DatabaseNotFoundError, SirannonError } from './errors.js'
import type {
  AfterQueryHook,
  BeforeConnectHook,
  BeforeQueryHook,
  DatabaseCloseHook,
  DatabaseOpenHook,
  DatabaseOptions,
  SirannonOptions,
} from './types.js'

export class Sirannon {
  private readonly dbs = new Map<string, Database>()
  private _shutdown = false

  constructor(readonly options?: SirannonOptions) {}

  open(id: string, path: string, options?: DatabaseOptions): Database {
    this.ensureRunning()
    if (this.dbs.has(id)) {
      throw new DatabaseAlreadyExistsError(id)
    }

    let db: Database
    try {
      db = new Database(id, path, options)
    } catch (err) {
      if (err instanceof SirannonError) throw err
      throw new SirannonError(
        `Failed to open database '${id}' at '${path}': ${err instanceof Error ? err.message : String(err)}`,
        'DATABASE_OPEN_FAILED',
      )
    }

    db.addCloseListener(() => this.dbs.delete(id))
    this.dbs.set(id, db)
    return db
  }

  close(id: string): void {
    this.ensureRunning()
    const db = this.dbs.get(id)
    if (!db) {
      throw new DatabaseNotFoundError(id)
    }
    db.close()
  }

  get(id: string): Database | undefined {
    return this.dbs.get(id)
  }

  has(id: string): boolean {
    return this.dbs.has(id)
  }

  databases(): Map<string, Database> {
    return new Map(this.dbs)
  }

  shutdown(): void {
    if (this._shutdown) return
    this._shutdown = true

    const errors: unknown[] = []
    const snapshot = [...this.dbs.values()]

    for (const db of snapshot) {
      try {
        db.close()
      } catch (err) {
        errors.push(err)
      }
    }

    this.dbs.clear()

    if (errors.length > 0) {
      throw new SirannonError(`Shutdown completed with ${errors.length} error(s)`, 'SHUTDOWN_ERROR')
    }
  }

  onBeforeQuery(_hook: BeforeQueryHook): void {
    throw new Error('not implemented')
  }

  onAfterQuery(_hook: AfterQueryHook): void {
    throw new Error('not implemented')
  }

  onBeforeConnect(_hook: BeforeConnectHook): void {
    throw new Error('not implemented')
  }

  onDatabaseOpen(_hook: DatabaseOpenHook): void {
    throw new Error('not implemented')
  }

  onDatabaseClose(_hook: DatabaseCloseHook): void {
    throw new Error('not implemented')
  }

  private ensureRunning(): void {
    if (this._shutdown) {
      throw new SirannonError('Sirannon has been shut down', 'SHUTDOWN')
    }
  }
}
