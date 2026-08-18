import type { DatabaseCopyRequest, DatabaseCopyStep } from '../../core/driver/types.js'
import { BackupError } from '../../core/errors.js'

const OPEN_TRANSACTION_MESSAGE =
  'A stepped copy cannot start while a transaction is open on the same connection, because SQLite copies no pages and reports success'

export async function copyDatabaseWithBetterSqlite3(
  db: import('better-sqlite3').Database,
  request: DatabaseCopyRequest,
): Promise<DatabaseCopyStep> {
  if (db.inTransaction) throw new BackupError(OPEN_TRANSACTION_MESSAGE)
  const final = await db.backup(request.destPath, {
    progress: step => {
      request.onStep?.(step)
      return request.pagesPerStep
    },
  })
  request.onStep?.(final)
  return final
}
