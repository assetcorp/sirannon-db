export interface HLCTimestamp {
  wallMs: number
  logical: number
  nodeId: string
}

export interface ReplicationChange {
  table: string
  operation: 'insert' | 'update' | 'delete' | 'ddl'
  rowId: string
  primaryKey: Record<string, unknown>
  hlc: string
  txId: string
  nodeId: string
  newData: Record<string, unknown> | null
  oldData: Record<string, unknown> | null
  ddlStatement?: string
}

export interface ReplicationBatch {
  sourceNodeId: string
  batchId: string
  fromSeq: bigint
  toSeq: bigint
  hlcRange: { min: string; max: string }
  changes: ReplicationChange[]
  checksum: string
  groupId?: string
  primaryTerm?: bigint
}

export interface ConflictContext {
  table: string
  rowId: string
  localChange: ReplicationChange | null
  remoteChange: ReplicationChange
  localHlc: string | null
  remoteHlc: string
}

export interface ConflictResolution {
  action: 'accept_remote' | 'keep_local' | 'merge'
  mergedData?: Record<string, unknown>
}

export interface ConflictResolver {
  resolve(ctx: ConflictContext): ConflictResolution | Promise<ConflictResolution>
}

export interface ApplyResult {
  applied: number
  skipped: number
  conflicts: number
  droppedTables: string[]
}

export interface SyncTableManifest {
  table: string
  rowCount: number
  pkHash?: string
  batchDigest?: string
}
