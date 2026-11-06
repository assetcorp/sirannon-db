import { rowCountsOf, withRowCounts } from '../driver/row-counts.js'
import type { Counted, GroupRunOutcome, SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { EncryptionError } from './errors.js'

const PAGE_AUTHENTICATION_FAILURE_CODE = /^SQLITE_(CORRUPT|NOTADB)/
const PAGE_AUTHENTICATION_FAILURE_MESSAGE = /database disk image is malformed|file is not a database/i
const PAGE_CORRUPT_MESSAGE =
  'A page of the encrypted database failed its authentication check, so Sirannon returned no rows from it. The file was changed or damaged outside SQLite; restore it from a backup'

function failsPageAuthentication(err: unknown): boolean {
  if (err instanceof SirannonError || typeof err !== 'object' || err === null) return false
  const { code, message } = err as { code?: unknown; message?: unknown }
  if (typeof code === 'string' && PAGE_AUTHENTICATION_FAILURE_CODE.test(code)) return true
  return typeof message === 'string' && PAGE_AUTHENTICATION_FAILURE_MESSAGE.test(message)
}

function asPageError(err: unknown): unknown {
  if (!failsPageAuthentication(err)) return err
  return withRowCounts(new EncryptionError(PAGE_CORRUPT_MESSAGE, 'ENCRYPTED_PAGE_CORRUPT', err), rowCountsOf(err))
}

async function guarded<T>(call: () => Promise<T>): Promise<T> {
  try {
    return await call()
  } catch (err) {
    throw asPageError(err)
  }
}

function guardedStatement(statement: SQLiteStatement): SQLiteStatement {
  const { allRaw, allCounted, getCounted, allRawCounted } = statement
  const result: SQLiteStatement = {
    all: <T>(...params: unknown[]) => guarded(() => statement.all<T>(...params)),
    get: <T>(...params: unknown[]) => guarded(() => statement.get<T>(...params)),
    run: (...params) => guarded(() => statement.run(...params)),
  }
  if (allRaw)
    result.allRaw = <T>(...params: unknown[]) =>
      guarded(() => allRaw.call<SQLiteStatement, unknown[], Promise<T[]>>(statement, ...params))
  if (allCounted) {
    result.allCounted = <T>(...params: unknown[]) =>
      guarded(() => allCounted.call<SQLiteStatement, unknown[], Promise<Counted<T[]>>>(statement, ...params))
  }
  if (getCounted) {
    result.getCounted = <T>(...params: unknown[]) =>
      guarded(() => getCounted.call<SQLiteStatement, unknown[], Promise<Counted<T | undefined>>>(statement, ...params))
  }
  if (allRawCounted) {
    result.allRawCounted = <T>(...params: unknown[]) =>
      guarded(() => allRawCounted.call<SQLiteStatement, unknown[], Promise<Counted<T[]>>>(statement, ...params))
  }
  return result
}

function guardedOutcomes(outcomes: GroupRunOutcome[]): GroupRunOutcome[] {
  return outcomes.map(outcome => {
    if (outcome.ok || !failsPageAuthentication(outcome.error)) return outcome
    return {
      ...outcome,
      error: {
        ...outcome.error,
        message: PAGE_CORRUPT_MESSAGE,
        name: 'EncryptionError',
        code: 'ENCRYPTED_PAGE_CORRUPT',
      },
    }
  })
}

/**
 * Returns a connection that reports every SQLite page authentication failure on an encrypted database as an
 * `EncryptionError` with `ENCRYPTED_PAGE_CORRUPT`, so that a caller never mistakes a damaged page for an ordinary
 * query error.
 *
 * @param connection - A connection that a driver opened under a key.
 * @returns A connection with the same members, whose failures carry the encryption code.
 * @internal
 */
export function reportEncryptedPageFailures(connection: SQLiteConnection): SQLiteConnection {
  const guardedConnection: SQLiteConnection = {
    exec: sql => guarded(() => connection.exec(sql)),
    prepare: async sql => guardedStatement(await guarded(() => connection.prepare(sql))),
    transaction: fn => guarded(() => connection.transaction(() => fn(guardedConnection))),
    close: () => connection.close(),
    ...(connection.copyRunsOffCallerThread === undefined
      ? {}
      : { copyRunsOffCallerThread: connection.copyRunsOffCallerThread }),
  }
  const {
    loadExtension,
    copyDatabase,
    runBatch,
    runBatchSummary,
    runGroup,
    startChangeCapture,
    takeCapturedChanges,
    stopChangeCapture,
    applyCapturedChanges,
  } = connection
  if (loadExtension) guardedConnection.loadExtension = path => loadExtension.call(connection, path)
  if (copyDatabase) guardedConnection.copyDatabase = request => guarded(() => copyDatabase.call(connection, request))
  if (runBatch) guardedConnection.runBatch = (sql, batch) => guarded(() => runBatch.call(connection, sql, batch))
  if (runBatchSummary) {
    guardedConnection.runBatchSummary = (sql, batch) => guarded(() => runBatchSummary.call(connection, sql, batch))
  }
  if (runGroup) {
    guardedConnection.runGroup = async (units, opening) =>
      guardedOutcomes(await guarded(() => runGroup.call(connection, units, opening)))
  }
  if (startChangeCapture) guardedConnection.startChangeCapture = () => startChangeCapture.call(connection)
  if (takeCapturedChanges) {
    guardedConnection.takeCapturedChanges = () => guarded(() => takeCapturedChanges.call(connection))
  }
  if (stopChangeCapture) guardedConnection.stopChangeCapture = () => stopChangeCapture.call(connection)
  if (applyCapturedChanges) {
    guardedConnection.applyCapturedChanges = changes => guarded(() => applyCapturedChanges.call(connection, changes))
  }
  return guardedConnection
}
