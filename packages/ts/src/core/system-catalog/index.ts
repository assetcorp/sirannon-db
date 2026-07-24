export { ensureChangesTable, maxChangeHlc, maxChangeSeq } from './changes-table.js'
export { assertSafeIdentifier, ensureColumn, tableColumns } from './columns.js'
export { ensureDeviceCursorsTable } from './device-cursors-table.js'
export { deleteMetaValue, ensureMetaTable, getMetaValue, initMetaValue, setMetaValue } from './meta-table.js'
export { ensureMigrationsTable } from './migrations-table.js'
export {
  ensureBatchApplyTables,
  ensureReplicationStateTables,
  prepareAppliedChangesInsert,
} from './replication-tables.js'
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
