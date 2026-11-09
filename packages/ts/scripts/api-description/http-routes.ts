import type { HttpWireTypeName } from '../../src/server/wire-types.js'

export type RouteMethod = 'get' | 'post'

export type RouteTag = 'Server' | 'Statements' | 'Operations' | 'Backups' | 'Encryption' | 'DeviceSync'

export interface OptionalJsonBody {
  optional: HttpWireTypeName
}

export type RouteRequestBody = HttpWireTypeName | OptionalJsonBody | null

export interface HttpRouteDescription {
  method: RouteMethod
  path: string
  operationId: string
  summary: string
  tag: RouteTag
  request: RouteRequestBody
  responses: Readonly<Record<number, HttpWireTypeName>>
}

export const HTTP_ROUTES: readonly HttpRouteDescription[] = [
  {
    method: 'get',
    path: '/capabilities',
    operationId: 'getCapabilities',
    summary:
      'Returns the capabilities that the server announces, such as query.sql when it accepts SQL statements, and the digest of its registered operations.',
    tag: 'Server',
    request: null,
    responses: { 200: 'CapabilitiesResponse' },
  },
  {
    method: 'get',
    path: '/health',
    operationId: 'getHealth',
    summary: 'Returns ok while the server process runs.',
    tag: 'Server',
    request: null,
    responses: { 200: 'LivenessResponse' },
  },
  {
    method: 'get',
    path: '/health/ready',
    operationId: 'getReadiness',
    summary: 'Returns whether each open database and the replication of this node can serve requests.',
    tag: 'Server',
    request: null,
    responses: { 200: 'ReadinessResponse' },
  },
  {
    method: 'get',
    path: '/db/:id/cluster',
    operationId: 'getClusterStatus',
    summary:
      'Returns the primary, the primary term, and the readable nodes of the database, to a caller that the server authorises for routing metadata.',
    tag: 'Server',
    request: null,
    responses: { 200: 'ClusterStatusResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/query',
    operationId: 'query',
    summary: 'Runs one read statement and returns its rows, on a server that accepts SQL.',
    tag: 'Statements',
    request: 'QueryRequest',
    responses: { 200: 'QueryResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/execute',
    operationId: 'execute',
    summary: 'Runs one write statement, on a server that accepts SQL.',
    tag: 'Statements',
    request: 'ExecuteRequest',
    responses: { 200: 'ExecuteResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/transaction',
    operationId: 'transaction',
    summary: 'Runs every statement in one transaction, so that all succeed or all fail, on a server that accepts SQL.',
    tag: 'Statements',
    request: 'TransactionRequest',
    responses: { 200: 'TransactionResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/batch',
    operationId: 'batch',
    summary: 'Runs one statement once for each parameter set, in one transaction, on a server that accepts SQL.',
    tag: 'Statements',
    request: 'BatchRequest',
    responses: { 200: 'BatchResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/load',
    operationId: 'load',
    summary: 'Loads rows while the writer runs at relaxed durability, on a server that accepts SQL.',
    tag: 'Statements',
    request: 'LoadRequest',
    responses: { 200: 'LoadResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/query/:name',
    operationId: 'queryOperation',
    summary: 'Runs a registered read by name and returns its rows.',
    tag: 'Operations',
    request: { optional: 'OperationQueryRequest' },
    responses: { 200: 'QueryResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/execute/:name',
    operationId: 'executeOperation',
    summary: 'Runs a registered write by name, in one transaction, and returns one result for each of its statements.',
    tag: 'Operations',
    request: { optional: 'OperationExecuteRequest' },
    responses: { 200: 'OperationExecuteResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/backup',
    operationId: 'triggerBackup',
    summary: 'Starts one turn of the backup checkpoint cycle and answers before the turn finishes.',
    tag: 'Backups',
    request: null,
    responses: { 202: 'BackupTriggerResponse' },
  },
  {
    method: 'get',
    path: '/db/:id/backup',
    operationId: 'getBackupStatus',
    summary: 'Returns what the backup cycle is doing and the results of its recent turns.',
    tag: 'Backups',
    request: null,
    responses: { 200: 'BackupStatusResponse' },
  },
  {
    method: 'get',
    path: '/db/:id/backup/chain',
    operationId: 'listBackupChains',
    summary: 'Returns every chain of backups that the destination stores.',
    tag: 'Backups',
    request: null,
    responses: { 200: 'BackupChainResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/backup/verify',
    operationId: 'verifyBackup',
    summary: 'Reads one stored backup back from the destination and checks it.',
    tag: 'Backups',
    request: 'BackupVerifyRequest',
    responses: { 200: 'BackupVerifyResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/backup/safe-to-delete',
    operationId: 'listBackupsSafeToDelete',
    summary: 'Returns the stored backups that no restore from the given moment onwards needs.',
    tag: 'Backups',
    request: { optional: 'BackupSafeToDeleteRequest' },
    responses: { 200: 'BackupSafeToDeleteResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/backup/restore',
    operationId: 'restoreBackup',
    summary: 'Starts rebuilding the database from its backups at a moment, on a server that accepts restores.',
    tag: 'Backups',
    request: { optional: 'BackupRestoreRequest' },
    responses: { 202: 'StartedResponse' },
  },
  {
    method: 'get',
    path: '/db/:id/backup/restore',
    operationId: 'getBackupRestoreStatus',
    summary: 'Returns how the last restore of the database went.',
    tag: 'Backups',
    request: null,
    responses: { 200: 'BackupRestoreStatus' },
  },
  {
    method: 'post',
    path: '/db/:id/encryption',
    operationId: 'changeEncryption',
    summary:
      'Starts encrypting the database, changing its key, or removing its encryption, on a server that accepts key changes.',
    tag: 'Encryption',
    request: 'EncryptionChangeRequest',
    responses: { 202: 'StartedResponse' },
  },
  {
    method: 'get',
    path: '/db/:id/encryption',
    operationId: 'getEncryptionStatus',
    summary: 'Returns how the database is encrypted and how its last key change went.',
    tag: 'Encryption',
    request: null,
    responses: { 200: 'EncryptionStatus' },
  },
  {
    method: 'post',
    path: '/db/:id/changes',
    operationId: 'applyChanges',
    summary: 'Applies a batch of changes that a device pushes, on a server that accepts device sync.',
    tag: 'DeviceSync',
    request: 'ChangesRequest',
    responses: { 200: 'ChangesResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/migrations',
    operationId: 'listMigrations',
    summary: 'Returns the migrations that the database has applied, on a server that accepts device sync.',
    tag: 'DeviceSync',
    request: 'MigrationListRequest',
    responses: { 200: 'MigrationListResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/snapshot',
    operationId: 'getSnapshotManifest',
    summary:
      'Returns the schema, tables, and starting sequence that a device copies the database from, on a server that accepts device sync.',
    tag: 'DeviceSync',
    request: null,
    responses: { 200: 'SnapshotManifestResponse' },
  },
  {
    method: 'post',
    path: '/db/:id/snapshot/page',
    operationId: 'getSnapshotPage',
    summary: 'Returns one page of a table for a device to copy, on a server that accepts device sync.',
    tag: 'DeviceSync',
    request: 'SnapshotPageRequest',
    responses: { 200: 'SnapshotPageResponse' },
  },
]
