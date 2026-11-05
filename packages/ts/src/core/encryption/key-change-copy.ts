import type { SQLiteConnection } from '../driver/types.js'
import { escId } from '../sql-identifiers.js'
import { EncryptionError } from './errors.js'
import { copyRowidsAfterSqlcipherExport } from './key-change-rowids.js'

/**
 * The schema name under which the snapshot connection attaches the new file of a key change.
 *
 * @internal
 */
export const KEY_CHANGE_SCHEMA = 'sirannon_key_change'

const ROWID_KEEPING_EXPORT = 'sirannon_export'
const SQLCIPHER_EXPORT = 'sqlcipher_export'
const HEX_DIGITS = '0123456789abcdef'
const QUOTE = 0x27
const LOWER_X = 0x78

function rawKeyspec(key: Uint8Array): Uint8Array {
  const spec = new Uint8Array(key.length * 2 + 3)
  spec[0] = LOWER_X
  spec[1] = QUOTE
  for (let i = 0; i < key.length; i++) {
    spec[2 + i * 2] = HEX_DIGITS.charCodeAt(key[i] >> 4)
    spec[3 + i * 2] = HEX_DIGITS.charCodeAt(key[i] & 0x0f)
  }
  spec[spec.length - 1] = QUOTE
  return spec
}

async function attachUnderKey(
  conn: SQLiteConnection,
  path: string,
  schema: string,
  key: Uint8Array | undefined,
): Promise<void> {
  const spec = key === undefined ? '' : rawKeyspec(key)
  try {
    await (await conn.prepare(`ATTACH DATABASE ? AS ${schema} KEY ?`)).run(path, spec)
  } finally {
    if (spec instanceof Uint8Array) spec.fill(0)
  }
}

async function readInteger(conn: SQLiteConnection, sql: string): Promise<bigint> {
  const row = (await (await conn.prepare(sql)).get()) as Record<string, unknown> | undefined
  const value = row === undefined ? undefined : Object.values(row)[0]
  if (typeof value === 'bigint') return value
  if (typeof value === 'number' && Number.isSafeInteger(value)) return BigInt(value)
  throw new EncryptionError(`SQLite returned no integer for '${sql}'`, 'ENCRYPTION_CHANGE_ABORTED')
}

/**
 * Returns the schema version of the main database on a connection, which SQLite raises on every schema change.
 *
 * @param conn - The connection.
 * @returns The schema version.
 * @internal
 */
export function readSchemaVersion(conn: SQLiteConnection): Promise<bigint> {
  return readInteger(conn, 'PRAGMA main.schema_version')
}

/**
 * Attaches the new file of a key change to the snapshot connection, under the target key or as plaintext, sets the
 * vacuum mode that the copy needs before SQLite writes any page of it, and turns foreign keys off so that no cascade
 * runs while the copy writes rows.
 *
 * @param snapshot - The snapshot connection.
 * @param newPath - The new file.
 * @param key - The target key, or `undefined` for a plaintext file.
 * @internal
 */
export async function attachNewFile(
  snapshot: SQLiteConnection,
  newPath: string,
  key: Uint8Array | undefined,
): Promise<void> {
  await attachUnderKey(snapshot, newPath, KEY_CHANGE_SCHEMA, key)
  const autoVacuum = await readInteger(snapshot, 'PRAGMA main.auto_vacuum')
  await snapshot.exec(`PRAGMA ${KEY_CHANGE_SCHEMA}.auto_vacuum = ${autoVacuum}`)
  await snapshot.exec('PRAGMA foreign_keys = OFF')
}

/**
 * Copies the snapshot that the connection's open read transaction holds into the attached new file, with every
 * rowid, and the header values that the copy leaves out.
 *
 * @param snapshot - The snapshot connection, inside the read transaction that pins the snapshot.
 * @returns The number of rows that the copy wrote.
 * @internal
 */
export async function exportSnapshot(snapshot: SQLiteConnection): Promise<number> {
  const exportFunction = (await (
    await snapshot.prepare(
      `SELECT name FROM pragma_function_list WHERE name IN ('${ROWID_KEEPING_EXPORT}', '${SQLCIPHER_EXPORT}') ORDER BY name = '${ROWID_KEEPING_EXPORT}' DESC LIMIT 1`,
    )
  ).get()) as { name?: unknown } | undefined
  if (exportFunction?.name !== ROWID_KEEPING_EXPORT && exportFunction?.name !== SQLCIPHER_EXPORT) {
    throw new EncryptionError(
      'The SQLite build has no function that copies a database under another key',
      'ENCRYPTION_UNSUPPORTED',
    )
  }
  await (await snapshot.prepare(`SELECT ${exportFunction.name}('${KEY_CHANGE_SCHEMA}')`)).get()
  if (exportFunction.name === SQLCIPHER_EXPORT) await copyRowidsAfterSqlcipherExport(snapshot, KEY_CHANGE_SCHEMA)
  const tables = (await (
    await snapshot.prepare(
      `SELECT name FROM ${KEY_CHANGE_SCHEMA}.sqlite_schema WHERE type = 'table' AND sql NOT LIKE 'CREATE VIRTUAL%'`,
    )
  ).all()) as { name: string }[]
  let rowsCopied = 0n
  for (const { name } of tables) {
    rowsCopied += await readInteger(snapshot, `SELECT count(*) FROM ${KEY_CHANGE_SCHEMA}."${escId(name)}"`)
  }
  const userVersion = await readInteger(snapshot, 'PRAGMA main.user_version')
  const applicationId = await readInteger(snapshot, 'PRAGMA main.application_id')
  await snapshot.exec(`PRAGMA ${KEY_CHANGE_SCHEMA}.user_version = ${userVersion}`)
  await snapshot.exec(`PRAGMA ${KEY_CHANGE_SCHEMA}.application_id = ${applicationId}`)
  return Number(rowsCopied)
}

/**
 * Detaches the new file from the snapshot connection.
 *
 * @param snapshot - The snapshot connection, with no transaction open.
 * @internal
 */
export async function detachNewFile(snapshot: SQLiteConnection): Promise<void> {
  await snapshot.exec(`DETACH DATABASE ${KEY_CHANGE_SCHEMA}`)
}

/**
 * The schema name under which the connection to the new file attaches the original file of a key change.
 *
 * @internal
 */
export const ORIGINAL_SCHEMA = 'sirannon_key_change_original'

const UNCAPTURED_TABLES = ['sqlite_sequence', 'sqlite_stat1', 'sqlite_stat4'] as const

/**
 * Attaches the original file of a key change to the connection to the new file, under the key that the original
 * holds, or as plaintext.
 *
 * @param target - The connection to the new file.
 * @param path - The original file.
 * @param key - The original's key, or `undefined` for a plaintext original.
 * @internal
 */
export function attachOriginal(target: SQLiteConnection, path: string, key: Uint8Array | undefined): Promise<void> {
  return attachUnderKey(target, path, ORIGINAL_SCHEMA, key)
}

/**
 * Detaches the original file from the connection to the new file.
 *
 * @param target - The connection to the new file, with no transaction open.
 * @internal
 */
export async function detachOriginal(target: SQLiteConnection): Promise<void> {
  await target.exec(`DETACH DATABASE ${ORIGINAL_SCHEMA}`)
}

async function tableExists(conn: SQLiteConnection, schema: string, name: string): Promise<boolean> {
  const row = await (await conn.prepare(`SELECT 1 FROM ${schema}.sqlite_schema WHERE type = 'table' AND name = ?`)).get(
    name,
  )
  return row !== undefined
}

/**
 * Replaces the new file's autoincrement counters and planner statistics with the original's, which no change
 * capture records, because SQLite keeps them in tables without a primary key and allows no trigger on them.
 *
 * @param target - The connection to the new file, with the original attached and writes to it held.
 * @throws An `EncryptionError` with `ENCRYPTION_CHANGE_ABORTED` when one file holds such a table and the other does not.
 * @internal
 */
export async function copyUncapturedTables(target: SQLiteConnection): Promise<void> {
  for (const name of UNCAPTURED_TABLES) {
    const inOriginal = await tableExists(target, ORIGINAL_SCHEMA, name)
    const inNewFile = await tableExists(target, 'main', name)
    if (inOriginal !== inNewFile) {
      throw new EncryptionError(
        `The table ${name} exists in only one of the original and the new file`,
        'ENCRYPTION_CHANGE_ABORTED',
      )
    }
    if (!inOriginal) continue
    await target.exec(`DELETE FROM main.${name}; INSERT INTO main.${name} SELECT * FROM ${ORIGINAL_SCHEMA}.${name}`)
  }
}
