/**
 * The base class for every sirannon-db error, which has a machine-readable
 * {@link SirannonError.code}. Extend it to define an error of your own.
 *
 * @public
 */
export class SirannonError extends Error {
  constructor(
    message: string,
    /**
     * The machine-readable code that the server maps to an HTTP status.
     */
    public readonly code: string,
  ) {
    super(message)
    this.name = 'SirannonError'
  }
}

/**
 * Thrown when the registry has no open database under an identifier, because
 * the database was never opened or is already closed.
 *
 * @public
 */
export class DatabaseNotFoundError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' not found`, 'DATABASE_NOT_FOUND')
    this.name = 'DatabaseNotFoundError'
  }
}

/**
 * Thrown when a caller opens a database under an identifier that the registry
 * already uses, since each identifier names one database.
 *
 * @public
 */
export class DatabaseAlreadyExistsError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' already exists`, 'DATABASE_ALREADY_EXISTS')
    this.name = 'DatabaseAlreadyExistsError'
  }
}

/**
 * Thrown when a caller writes to a database that is open in read-only mode.
 *
 * @public
 */
export class ReadOnlyError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' is read-only`, 'READ_ONLY')
    this.name = 'ReadOnlyError'
  }
}

/**
 * Thrown when SQLite fails to execute a statement, with that statement in
 * {@link QueryError.sql} for your logs.
 *
 * @public
 */
export class QueryError extends SirannonError {
  constructor(
    message: string,
    /**
     * The statement that failed.
     */
    public readonly sql: string,
  ) {
    super(message, 'QUERY_ERROR')
    this.name = 'QueryError'
  }
}

/**
 * Reports a transaction that failed to commit or rolled back, with the cause in
 * the message. The server responds to its `TRANSACTION_ERROR` code with status 400.
 *
 * @public
 */
export class TransactionError extends SirannonError {
  constructor(message: string) {
    super(message, 'TRANSACTION_ERROR')
    this.name = 'TransactionError'
  }
}

/**
 * Thrown when a migration fails, with the version of that migration in
 * {@link MigrationError.version}.
 *
 * @public
 */
export class MigrationError extends SirannonError {
  constructor(
    message: string,
    /**
     * The version of the migration that failed.
     */
    public readonly version: number,
    code: string = 'MIGRATION_ERROR',
  ) {
    super(message, code)
    this.name = 'MigrationError'
  }
}

/**
 * An error that a before-hook can throw to reject an operation, which the
 * server responds to with status 403. The message includes the optional
 * `reason`, so that a caller can tell one hook policy from another.
 *
 * @public
 */
export class HookDeniedError extends SirannonError {
  constructor(hookName: string, reason?: string) {
    super(
      reason ? `Hook '${hookName}' denied the operation: ${reason}` : `Hook '${hookName}' denied the operation`,
      'HOOK_DENIED',
    )
    this.name = 'HookDeniedError'
  }
}

/**
 * Rejects one request with a status of your own; throw it from an authenticate hook or a registered operation.
 *
 * @public
 */
export class RequestDeniedError extends SirannonError {
  /**
   * The HTTP status that the server responds to the rejected request with.
   */
  readonly status: number

  constructor(status: number, code: string, message: string) {
    super(message, code)
    this.name = 'RequestDeniedError'
    this.status = status
  }
}

/**
 * Thrown when change capture cannot proceed, for example when a caller watches
 * a table that does not exist, when Sirannon cannot read the change-log epoch
 * or the node identity, or when a live query uses a statement that Sirannon
 * cannot keep current.
 *
 * @public
 */
export class CDCError extends SirannonError {
  constructor(message: string) {
    super(message, 'CDC_ERROR')
    this.name = 'CDCError'
  }
}

/**
 * Thrown when a statement through the query API names a `_sirannon` table,
 * modifies a `sqlite_` table, contains ATTACH or DETACH, or sets
 * `PRAGMA writable_schema`. Sirannon rejects these statements to keep its
 * change log, its replication ledger, and SQLite's schema catalogue out of the
 * caller's reach.
 *
 * @public
 */
export class ForbiddenSqlError extends SirannonError {
  constructor(message: string) {
    super(message, 'FORBIDDEN_SQL')
    this.name = 'ForbiddenSqlError'
  }
}

/**
 * Thrown when a copy to a file or a scheduled backup fails, or when its path,
 * cron expression, or time zone is invalid.
 *
 * @public
 */
export class BackupError extends SirannonError {
  constructor(message: string) {
    super(message, 'BACKUP_ERROR')
    this.name = 'BackupError'
  }
}

/**
 * Thrown when a caller asks a closed or empty connection pool for a connection, asks a
 * read-only pool for the writer, or closes a pool and one of its connections
 * fails to close.
 *
 * @public
 */
export class ConnectionPoolError extends SirannonError {
  constructor(message: string) {
    super(message, 'CONNECTION_POOL_ERROR')
    this.name = 'ConnectionPoolError'
  }
}

/**
 * Thrown when the lifecycle resolver would open a database past `maxOpen`, and
 * evicting the least recently used database frees no slot. Close a database
 * before you open another one.
 *
 * @public
 */
export class MaxDatabasesError extends SirannonError {
  constructor(max: number) {
    super(`Maximum number of open databases (${max}) reached`, 'MAX_DATABASES')
    this.name = 'MaxDatabasesError'
  }
}

/**
 * Thrown when more writes are pending than the writer-worker limit allows, so
 * that the database sheds load. The server responds to it with status 503 and
 * a `Retry-After` header.
 *
 * @public
 */
export class WriteOverloadError extends SirannonError {
  constructor(
    /**
     * The number of pending writes that the database accepts before it rejects more.
     */
    public readonly limit: number,
    /**
     * The number of milliseconds that the caller should wait before it retries.
     */
    public readonly retryAfterMs: number,
  ) {
    super(`Write rejected: ${limit} writes already pending`, 'WRITE_OVERLOADED')
    this.name = 'WriteOverloadError'
  }
}

/**
 * Thrown when Sirannon cannot load a compiled SQLite extension. The message
 * names the extension path, and the optional `cause` gives the reason that
 * SQLite or Sirannon reported.
 *
 * @public
 */
export class ExtensionError extends SirannonError {
  constructor(path: string, cause?: string) {
    super(
      cause ? `Failed to load extension '${path}': ${cause}` : `Failed to load extension '${path}'`,
      'EXTENSION_ERROR',
    )
    this.name = 'ExtensionError'
  }
}
