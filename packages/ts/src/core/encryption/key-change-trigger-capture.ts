import {
  dropKeyChangeCaptureTriggers,
  installKeyChangeCaptureTriggers,
  type KeyChangeCaptureTriggerNames,
} from '../cdc/trigger-sql.js'
import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { escId } from '../sql-identifiers.js'
import {
  deleteKeyChangeLogKeys,
  dropKeyChangeLogTable,
  ensureEmptyKeyChangeTouchedTable,
  ensureKeyChangeLogTable,
  type KeyChangeCaptureTables,
  keyChangeCaptureTables,
  keyChangeCaptureTriggerName,
  prepareInsertKeyChangeTouchedKey,
  selectDistinctKeyChangeLogKeys,
  selectKeyChangeTouchedKeysSql,
} from '../system-catalog/key-change-capture-tables.js'
import { ORIGINAL_SCHEMA } from './key-change-copy.js'
import { assertSameWriter, type CapturedRound, type KeyChangeCapture, keyChangeAborted } from './key-change-round.js'
import { freeRowidAlias } from './key-change-rowids.js'

const SMALL_TRIGGER_ROUND_KEYS = 1024

interface TableListRow {
  name: string
  type: string
  wr: unknown
}

interface ColumnRow {
  name: string
  pk: unknown
  hidden: unknown
}

interface CapturedTable {
  readonly name: string
  readonly quoted: string
  readonly keys: readonly string[]
  readonly columns: readonly string[]
  readonly capture: KeyChangeCaptureTables
  readonly triggers: KeyChangeCaptureTriggerNames
}

type KeyRow = Record<string, unknown>

function quoted(name: string): string {
  return `"${escId(name)}"`
}

async function describeTable(writer: SQLiteConnection, row: TableListRow, position: number): Promise<CapturedTable> {
  const columns = (await (await writer.prepare(`PRAGMA main.table_xinfo(${quoted(row.name)})`)).all()) as ColumnRow[]
  const stored = columns.filter(column => Number(column.hidden) === 0).map(column => quoted(column.name))
  const withoutRowid = Number(row.wr) === 1
  const alias = withoutRowid ? null : freeRowidAlias(row.name, columns)
  const keys =
    alias === null
      ? columns
          .filter(column => Number(column.pk) > 0)
          .sort((a, b) => Number(a.pk) - Number(b.pk))
          .map(column => quoted(column.name))
      : [alias]
  return {
    name: row.name,
    quoted: quoted(row.name),
    keys,
    columns: alias === null ? stored : [alias, ...stored],
    capture: keyChangeCaptureTables(position, keys.length),
    triggers: {
      insert: keyChangeCaptureTriggerName(position, 'insert'),
      update: keyChangeCaptureTriggerName(position, 'update'),
      delete: keyChangeCaptureTriggerName(position, 'delete'),
    },
  }
}

async function installCapture(writer: SQLiteConnection, table: CapturedTable): Promise<void> {
  await ensureKeyChangeLogTable(writer, table.capture)
  await installKeyChangeCaptureTriggers(writer, table.triggers, table.name, table.capture.log, table.keys)
}

async function drainTable(writer: SQLiteConnection, table: CapturedTable): Promise<KeyRow[]> {
  const keys = await selectDistinctKeyChangeLogKeys(writer, table.capture)
  if (keys.length > 0) await deleteKeyChangeLogKeys(writer, table.capture)
  return keys
}

async function stageKeys(target: SQLiteConnection, table: CapturedTable, keys: readonly KeyRow[]): Promise<void> {
  await ensureEmptyKeyChangeTouchedTable(target, table.capture)
  const insert = await prepareInsertKeyChangeTouchedKey(target, table.capture)
  for (const key of keys) {
    await insert.run(...table.capture.slots.flatMap(slot => [key[`t_${slot}`], key[`v_${slot}`], key[`v_${slot}`]]))
  }
}

function touchedRows(table: CapturedTable): string {
  return `(${table.keys.join(', ')}) IN (${selectKeyChangeTouchedKeysSql(table.capture)})`
}

async function applyKeys(target: SQLiteConnection, drained: ReadonlyMap<CapturedTable, KeyRow[]>): Promise<void> {
  if (drained.size === 0) return
  await target.exec('BEGIN IMMEDIATE')
  try {
    for (const [table, keys] of drained) await stageKeys(target, table, keys)
    for (const table of drained.keys())
      await target.exec(`DELETE FROM main.${table.quoted} WHERE ${touchedRows(table)}`)
    for (const table of drained.keys()) {
      const columns = table.columns.join(', ')
      await target.exec(
        `INSERT INTO main.${table.quoted} (${columns}) SELECT ${columns} FROM ${ORIGINAL_SCHEMA}.${table.quoted} WHERE ${touchedRows(table)}`,
      )
    }
    await target.exec('COMMIT')
  } catch (err) {
    await target.exec('ROLLBACK').catch(() => undefined)
    if (err instanceof SirannonError) throw err
    throw keyChangeAborted('A write committed during the key change could not be applied exactly to the new file', err)
  }
}

async function rowCount(conn: SQLiteConnection, schema: string, table: string): Promise<string> {
  const row = (await (await conn.prepare(`SELECT CAST(count(*) AS TEXT) AS n FROM ${schema}.${table}`)).get()) as {
    n: string
  }
  return row.n
}

async function copyShadowTables(target: SQLiteConnection, shadows: readonly string[]): Promise<void> {
  for (const shadow of shadows) {
    await target.exec(
      `DELETE FROM main.${shadow}; INSERT INTO main.${shadow} SELECT * FROM ${ORIGINAL_SCHEMA}.${shadow}`,
    )
  }
}

async function assertCountsMatch(target: SQLiteConnection, tables: readonly string[]): Promise<void> {
  for (const table of tables) {
    if ((await rowCount(target, 'main', table)) !== (await rowCount(target, ORIGINAL_SCHEMA, table))) {
      throw keyChangeAborted(
        `The new file holds a different number of rows in ${table} than the original, because a write removed rows without running its triggers`,
      )
    }
  }
}

/**
 * Starts a capture that records, through temporary triggers on the writer, the key of every row that a write
 * touches, so that each round copies those rows from the original into the new file. The final round copies the
 * shadow tables of every virtual table in full, since SQLite allows no trigger on them, and checks that every table
 * holds as many rows in both files.
 *
 * @param writer - The original's writer, with writes held.
 * @returns The capture.
 * @internal
 */
export async function startTriggerCapture(writer: SQLiteConnection): Promise<KeyChangeCapture> {
  const listed = (await (
    await writer.prepare(
      "SELECT name, type, wr FROM pragma_table_list WHERE schema = 'main' AND type IN ('table', 'shadow') AND name NOT LIKE 'sqlite_%'",
    )
  ).all()) as TableListRow[]
  const regular = listed.filter(row => row.type === 'table')
  const shadows = listed.filter(row => row.type === 'shadow').map(row => quoted(row.name))
  const tables: CapturedTable[] = []
  for (const row of regular) tables.push(await describeTable(writer, row, tables.length))
  const stop = async () => {
    for (const table of tables) {
      await dropKeyChangeCaptureTriggers(writer, table.triggers)
      await dropKeyChangeLogTable(writer, table.capture)
    }
  }
  try {
    for (const table of tables) await installCapture(writer, table)
  } catch (err) {
    await stop().catch(() => undefined)
    throw err
  }
  return {
    async drain(current): Promise<CapturedRound> {
      assertSameWriter(writer, current)
      const drained = new Map<CapturedTable, KeyRow[]>()
      let total = 0
      for (const table of tables) {
        const keys = await drainTable(writer, table)
        if (keys.length === 0) continue
        drained.set(table, keys)
        total += keys.length
      }
      return { small: total <= SMALL_TRIGGER_ROUND_KEYS, apply: target => applyKeys(target, drained) }
    },
    async finish(target) {
      await copyShadowTables(target, shadows)
      await assertCountsMatch(target, [...tables.map(table => table.quoted), ...shadows])
    },
    stop,
  }
}
