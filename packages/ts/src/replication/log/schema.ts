import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { INTERNAL_TABLE_PREFIX, SYNC_STATE_TABLE } from '../../core/internal-tables.js'
import { dumpSchema, tablesInFkOrder } from '../../core/sync/schema-dump.js'
import { IDENTIFIER_RE } from '../../core/sync/validators.js'
import {
  ensureChangesTable,
  ensureMetaTable,
  ensureReplicationStateTables,
  setForeignKeysEnabled,
} from '../../core/system-catalog/index.js'
import { ReplicationError } from '../errors.js'

export class SchemaOps {
  constructor(
    private readonly conn: SQLiteConnection,
    private readonly changesTable: string,
  ) {}

  async ensureReplicationTables(): Promise<void> {
    await ensureChangesTable(this.conn, this.changesTable)
    await ensureReplicationStateTables(this.conn)
    await ensureMetaTable(this.conn)
  }

  async dumpSchema(conn: SQLiteConnection, excludePrefix: string = INTERNAL_TABLE_PREFIX): Promise<string[]> {
    return dumpSchema(conn, excludePrefix)
  }

  async getTablesInFkOrder(conn: SQLiteConnection): Promise<string[]> {
    return tablesInFkOrder(conn)
  }

  async wipeTables(conn: SQLiteConnection, tables: string[], tracker: ChangeTracker): Promise<void> {
    for (const table of tables) {
      if (!IDENTIFIER_RE.test(table)) {
        throw new ReplicationError(`Invalid table name: ${table}`)
      }
    }

    await setForeignKeysEnabled(conn, false)
    try {
      await conn.transaction(async tx => {
        for (const table of tables) {
          await tracker.unwatch(tx, table)
        }
        const reversed = [...tables].reverse()
        for (const table of reversed) {
          await tx.exec(`DELETE FROM "${table}"`)
        }
        await tx.exec(`DELETE FROM ${SYNC_STATE_TABLE} WHERE table_name != '__sync_meta__'`)
      })
    } finally {
      await setForeignKeysEnabled(conn, true)
    }
  }
}
