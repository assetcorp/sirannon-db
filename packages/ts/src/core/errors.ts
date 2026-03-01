/**
 * Base class for all sirannon-db errors. Extend this class to create
 * domain-specific errors that carry a machine-readable {@link code}.
 */
export class SirannonError extends Error {
  constructor(
    message: string,
    public readonly code: string,
  ) {
    super(message)
    this.name = 'SirannonError'
  }
}

/**
 * Thrown when a database ID cannot be resolved in the registry.
 * This typically means the database was never opened or has already been closed.
 */
export class DatabaseNotFoundError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' not found`, 'DATABASE_NOT_FOUND')
    this.name = 'DatabaseNotFoundError'
  }
}

/**
 * Thrown when attempting to register a database with an ID that is already
 * in use. Each database ID must be unique within the registry.
 */
export class DatabaseAlreadyExistsError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' already exists`, 'DATABASE_ALREADY_EXISTS')
    this.name = 'DatabaseAlreadyExistsError'
  }
}

/**
 * Thrown when a write operation is attempted on a database that was opened
 * in read-only mode.
 */
export class ReadOnlyError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' is read-only`, 'READ_ONLY')
    this.name = 'ReadOnlyError'
  }
}

/**
 * Thrown when SQLite fails to execute a statement. The {@link sql} property
 * holds the original SQL string that caused the failure, which is useful for
 * debugging and logging.
 */
export class QueryError extends SirannonError {
  constructor(
    message: string,
    public readonly sql: string,
  ) {
    super(message, 'QUERY_ERROR')
    this.name = 'QueryError'
  }
}

/**
 * Thrown when a transaction cannot be committed or is forcibly rolled back.
 * Check the message for the underlying cause.
 */
export class TransactionError extends SirannonError {
  constructor(message: string) {
    super(message, 'TRANSACTION_ERROR')
    this.name = 'TransactionError'
  }
}

/**
 * Thrown when a migration step fails. The {@link version} property identifies
 * which schema version triggered the error so the failure can be pinpointed
 * in the migration history.
 */
export class MigrationError extends SirannonError {
  constructor(
    message: string,
    public readonly version: number,
  ) {
    super(message, 'MIGRATION_ERROR')
    this.name = 'MigrationError'
  }
}

/**
 * Thrown when a before-hook explicitly rejects an operation. The optional
 * `reason` string is surfaced in the message so callers can distinguish
 * between different hook policies.
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
 * Thrown when the change-data-capture pipeline encounters an unrecoverable
 * error, such as a failed event dispatch or a corrupt change record.
 */
export class CDCError extends SirannonError {
  constructor(message: string) {
    super(message, 'CDC_ERROR')
    this.name = 'CDCError'
  }
}

/**
 * Thrown when a backup operation fails, whether that is an online backup via
 * the SQLite backup API or a file-level copy.
 */
export class BackupError extends SirannonError {
  constructor(message: string) {
    super(message, 'BACKUP_ERROR')
    this.name = 'BackupError'
  }
}

/**
 * Thrown when the connection pool reaches its limit or is configured with
 * invalid parameters such as a minimum size greater than the maximum.
 */
export class ConnectionPoolError extends SirannonError {
  constructor(message: string) {
    super(message, 'CONNECTION_POOL_ERROR')
    this.name = 'ConnectionPoolError'
  }
}

/**
 * Thrown when opening a new database would exceed the configured cap on
 * concurrently open databases. Close an existing database before opening
 * another one.
 */
export class MaxDatabasesError extends SirannonError {
  constructor(max: number) {
    super(`Maximum number of open databases (${max}) reached`, 'MAX_DATABASES')
    this.name = 'MaxDatabasesError'
  }
}

/**
 * Thrown when a native SQLite extension cannot be loaded. The `path` argument
 * is the filesystem path passed to `load_extension`, and the optional `cause`
 * string carries the error detail reported by SQLite.
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
