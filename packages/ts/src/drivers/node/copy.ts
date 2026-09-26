import type { DatabaseSync } from 'node:sqlite'
import type { DatabaseCopyRequest, DatabaseCopyStep } from '../../core/driver/types.js'
import { BackupError, SirannonError } from '../../core/errors.js'

const OPEN_TRANSACTION_MESSAGE =
  'A stepped copy cannot start while a transaction is open on the same connection, because SQLite copies no pages and reports success'

const NO_NODE_SQLITE_BACKUP_MESSAGE =
  'This Node.js build provides no backup function in node:sqlite, so the Node driver cannot copy a database. Upgrade Node.js to a release whose node:sqlite exports backup.'

export async function copyDatabaseWithNodeSqlite(
  db: DatabaseSync,
  request: DatabaseCopyRequest,
): Promise<DatabaseCopyStep> {
  if (db.isTransaction) throw new BackupError(OPEN_TRANSACTION_MESSAGE)
  const { backup } = await import('node:sqlite')
  if (typeof backup !== 'function') throw new SirannonError(NO_NODE_SQLITE_BACKUP_MESSAGE, 'BACKUP_UNSUPPORTED')
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
