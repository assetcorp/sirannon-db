import { betterSqlite3 } from 'sirannon-db-released/driver/better-sqlite3'

export { ChangeTracker, Sirannon } from 'sirannon-db-released'
export {
  coordinatorBackupGroup,
  PrimaryReplicaTopology,
  ReplicationEngine,
  toClusterStatusInfo,
  toReplicationStatusInfo,
} from 'sirannon-db-released/replication'
export { createEtcdCoordinator } from 'sirannon-db-released/replication/coordinator/etcd'
export { createServer } from 'sirannon-db-released/server'
export { GrpcReplicationTransport } from 'sirannon-db-released/transport/grpc'

export const testDriver = betterSqlite3()
