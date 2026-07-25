export {
  appliedSourceSeqsInRange,
  ensureAppliedChangesTable,
  maxAppliedSourceSeq,
  maxAppliedSourceSeqByNode,
  prepareAppliedChangesInsert,
} from './applied-changes-table.js'
export {
  countOutboundChanges,
  deleteChangesBeforeSql,
  deleteChangesBeforeUpToSeqSql,
  ensureChangesTable,
  maxChangeHlc,
  maxChangeSeq,
  maxRowChangeHlc,
  minChangeSeqSql,
  nextForeignChangeSeqSql,
  recordDdlChange,
  selectChangesAfterSeqSql,
  selectNodeChangesAfterSeqSql,
  selectOutboundChangesSql,
  selectTableChangesInRangeSql,
  selectTablesChangesInRangeSql,
  stampChangesAfterSeqSql,
  stampUnstampedChangesSql,
} from './changes-table.js'
export {
  ensureColumnVersionsTable,
  maxColumnVersionHlc,
  maxColumnVersionHlcForRow,
  prepareColumnVersionRowDelete,
  prepareColumnVersionRowDeleteUpToHlc,
  prepareColumnVersionUpsert,
  prepareNewerColumnVersionUpsert,
} from './column-versions-table.js'
export { assertSafeIdentifier, ensureColumn, tableColumns } from './columns.js'
export {
  type DeviceCursorRow,
  deleteDeviceCursorsUpdatedBefore,
  deviceCursorRows,
  ensureDeviceCursorsTable,
  upsertDeviceCursor,
} from './device-cursors-table.js'
export {
  deleteMetaValue,
  ensureMetaTable,
  getMetaValue,
  initMetaValue,
  SET_META_VALUE_SQL,
  setMetaValue,
} from './meta-table.js'
export {
  type AppliedMigrationRow,
  appliedMigrationEntriesNewestFirst,
  appliedMigrationRows,
  ensureMigrationsTable,
  highestMigrationVersion,
  type MigrationEntryRow,
  prepareMigrationChecksumUpdate,
  prepareMigrationDelete,
  prepareMigrationInsert,
  replaceMigrationHistory,
} from './migrations-table.js'
export { ensurePeerStateTable, minPeerAckedSeq, peerAckedSeq, upsertPeerAckedSeq } from './peer-state-table.js'
export { ensureBatchApplyTables, ensureReplicationStateTables } from './replication-tables.js'
export {
  countTableRows,
  referencedTables,
  type SchemaObjectRow,
  setForeignKeysEnabled,
  type TableInfoRow,
  tableColumnNames,
  tableExists,
  tableInfoRows,
  tablePkColumns,
  userSchemaObjects,
  userTableNames,
} from './sqlite-catalog.js'
export {
  completedSyncTableNames,
  deleteSyncTableStates,
  ensureSyncStateTable,
  type SyncMetaRow,
  syncMetaRow,
  upsertSyncMeta,
  upsertSyncTableStatus,
} from './sync-state-table.js'
