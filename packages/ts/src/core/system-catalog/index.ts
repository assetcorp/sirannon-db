export {
  ensureAppliedChangesTable,
  prepareInsertAppliedChange,
  selectAppliedSourceSeqsInRange,
  selectMaxAppliedSourceSeq,
  selectMaxAppliedSourceSeqByNode,
} from './applied-changes-table.js'
export {
  deleteChangesBeforeSql,
  deleteChangesBeforeUpToSeqSql,
  ensureChangesTable,
  insertDdlChange,
  selectChangesAfterSeqSql,
  selectCountOutboundChanges,
  selectMaxChangeHlc,
  selectMaxChangeSeq,
  selectMaxRowChangeHlc,
  selectMinChangeSeqSql,
  selectMinForeignChangeSeqSql,
  selectNodeChangesAfterSeqSql,
  selectOutboundChangesSql,
  selectTableChangesInRangeSql,
  selectTablesChangesInRangeSql,
  updateChangeStampsAfterSeqSql,
  updateUnstampedChangeStampsSql,
} from './changes-table.js'
export {
  ensureColumnVersionsTable,
  prepareDeleteRowColumnVersions,
  prepareDeleteRowColumnVersionsUpToHlc,
  prepareUpsertColumnVersion,
  prepareUpsertNewerColumnVersion,
  selectMaxColumnVersionHlc,
  selectMaxColumnVersionHlcForRow,
} from './column-versions-table.js'
export { assertSafeIdentifier, ensureColumn, tableColumns } from './columns.js'
export {
  type DeviceCursorRow,
  deleteDeviceCursorsUpdatedBefore,
  ensureDeviceCursorsTable,
  selectDeviceCursors,
  upsertDeviceCursor,
} from './device-cursors-table.js'
export {
  deleteMetaValue,
  ensureMetaTable,
  insertMetaValueIfAbsent,
  selectMetaValue,
  UPSERT_META_VALUE_SQL,
  upsertMetaValue,
} from './meta-table.js'
export {
  type AppliedMigrationRow,
  ensureMigrationsTable,
  highestMigrationVersion,
  type MigrationEntryRow,
  prepareDeleteMigration,
  prepareInsertMigration,
  prepareUpdateMigrationChecksum,
  replaceMigrationHistory,
  selectAppliedMigrations,
  selectAppliedMigrationsNewestFirst,
} from './migrations-table.js'
export {
  ensurePeerStateTable,
  selectMinPeerAckedSeq,
  selectPeerAckedSeq,
  upsertPeerAckedSeq,
} from './peer-state-table.js'
export { ensureBatchApplyTables, ensureReplicationStateTables } from './replication-tables.js'
export {
  referencedTables,
  type SchemaObjectRow,
  selectCountTableRows,
  selectTableExists,
  selectUserSchemaObjects,
  selectUserTableNames,
  setForeignKeysEnabled,
  type TableInfoRow,
  tableColumnNames,
  tableInfoRows,
  tablePkColumns,
} from './sqlite-catalog.js'
export {
  deleteSyncTableStates,
  ensureSyncStateTable,
  type SyncMetaRow,
  selectCompletedSyncTableNames,
  selectSyncMetaRow,
  upsertSyncMeta,
  upsertSyncTableStatus,
} from './sync-state-table.js'
