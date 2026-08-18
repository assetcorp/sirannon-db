import type { DatabaseSync } from 'node:sqlite'
import type { DatabaseCopyRequest, DatabaseCopyStep } from '../../core/driver/types.js'
import { BackupError } from '../../core/errors.js'

const OPEN_TRANSACTION_MESSAGE =
  'A stepped copy cannot start while a transaction is open on the same connection, because SQLite copies no pages and reports success'

export async function copyDatabaseWithNodeSqlite(
  db: DatabaseSync,
  request: DatabaseCopyRequest,
): Promise<DatabaseCopyStep> {
  if (db.isTransaction) throw new BackupError(OPEN_TRANSACTION_MESSAGE)
  const { backup } = await import('node:sqlite')
  let totalPages = 0
  const pageCount = await backup(db, request.destPath, {
    rate: request.pagesPerStep,
    progress: step => {
      totalPages = step.totalPages
      request.onStep?.(step)
    },
  })
  const final = { totalPages: Math.max(totalPages, pageCount), remainingPages: 0 }
  request.onStep?.(final)
  return final
}
