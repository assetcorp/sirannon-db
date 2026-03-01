// ---------------------------------------------------------------------------
// Error types for sirannon-db
// ---------------------------------------------------------------------------

/** Base error class for all sirannon-db errors. */
export class SirannonError extends Error {
  constructor(
    message: string,
    public readonly code: string,
  ) {
    super(message)
    this.name = 'SirannonError'
  }
}

/** Thrown when a database with the given ID is not found in the registry. */
export class DatabaseNotFoundError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' not found`, 'DATABASE_NOT_FOUND')
    this.name = 'DatabaseNotFoundError'
  }
}

/** Thrown when a database with the given ID already exists in the registry. */
export class DatabaseAlreadyExistsError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' already exists`, 'DATABASE_ALREADY_EXISTS')
    this.name = 'DatabaseAlreadyExistsError'
  }
}

/** Thrown when attempting to write to a read-only database. */
export class ReadOnlyError extends SirannonError {
  constructor(id: string) {
    super(`Database '${id}' is read-only`, 'READ_ONLY')
    this.name = 'ReadOnlyError'
  }
}

/** Thrown when a query fails to execute. */
export class QueryError extends SirannonError {
  constructor(
    message: string,
    public readonly sql: string,
  ) {
    super(message, 'QUERY_ERROR')
    this.name = 'QueryError'
  }
}

/** Thrown when a transaction fails. */
export class TransactionError extends SirannonError {
  constructor(message: string) {
    super(message, 'TRANSACTION_ERROR')
    this.name = 'TransactionError'
  }
}

/** Thrown when a migration fails. */
export class MigrationError extends SirannonError {
  constructor(
    message: string,
    public readonly version: number,
  ) {
    super(message, 'MIGRATION_ERROR')
    this.name = 'MigrationError'
  }
}

/** Thrown when a hook denies an operation. */
export class HookDeniedError extends SirannonError {
  constructor(hookName: string, reason?: string) {
    super(
      reason ? `Hook '${hookName}' denied the operation: ${reason}` : `Hook '${hookName}' denied the operation`,
      'HOOK_DENIED',
    )
    this.name = 'HookDeniedError'
  }
}

/** Thrown when a CDC operation fails. */
export class CDCError extends SirannonError {
  constructor(message: string) {
    super(message, 'CDC_ERROR')
    this.name = 'CDCError'
  }
}

/** Thrown when a backup operation fails. */
export class BackupError extends SirannonError {
  constructor(message: string) {
    super(message, 'BACKUP_ERROR')
    this.name = 'BackupError'
  }
}

/** Thrown when the connection pool is exhausted or misconfigured. */
export class ConnectionPoolError extends SirannonError {
  constructor(message: string) {
    super(message, 'CONNECTION_POOL_ERROR')
    this.name = 'ConnectionPoolError'
  }
}

/** Thrown when the maximum number of open databases is reached. */
export class MaxDatabasesError extends SirannonError {
  constructor(max: number) {
    super(`Maximum number of open databases (${max}) reached`, 'MAX_DATABASES')
    this.name = 'MaxDatabasesError'
  }
}

/** Thrown when an extension fails to load. */
export class ExtensionError extends SirannonError {
  constructor(path: string, cause?: string) {
    super(
      cause ? `Failed to load extension '${path}': ${cause}` : `Failed to load extension '${path}'`,
      'EXTENSION_ERROR',
    )
    this.name = 'ExtensionError'
  }
}
