import type { HttpRequest, HttpResponse } from 'uWebSockets.js'
import type { Sirannon } from '../core/sirannon.js'
import type { NodeHealthReason, ReplicationStatusInfo } from '../core/types.js'
import { endResponse } from './connection-drain.js'

/**
 * The body of `GET /health`, which the server sends while its process runs.
 *
 * @public
 */
export interface LivenessResponse {
  /** Always `ok`, because a process that cannot answer sends nothing. */
  status: 'ok'
}

/**
 * Whether one open database can serve requests.
 *
 * @public
 */
export interface DatabaseStatus {
  /** The identifier under which the registry holds the database. */
  id: string
  /** Whether the database refuses writes. */
  readOnly: boolean
  /** Whether the database has closed, which makes the readiness status `degraded`. */
  closed: boolean
}

/**
 * The body of `GET /health/ready`, which states whether the node can serve reads and writes.
 *
 * @public
 */
export interface ReadinessResponse {
  /**
   * What the node can do now: `ok` when it is healthy, `degraded` when a database has closed, and the replication
   * health state otherwise.
   */
  status: 'ok' | 'degraded' | 'failing_over' | 'unavailable' | 'repairing' | 'syncing'
  /** One entry for each database that the registry holds open. */
  databases: DatabaseStatus[]
  /** The replication state of this node, which the server reports when it runs with replication. */
  replication?: ReplicationReadiness
}

/**
 * The replication state of one node, as `GET /health/ready` reports it.
 *
 * @public
 */
export interface ReplicationReadiness {
  /** Whether this node is the primary or a replica. */
  role: string
  /** Whether this node forwards the writes that it receives to the primary. */
  writeForwarding: boolean
  /** How many peers this node replicates with. */
  peers: number
  /** The highest change sequence that this node has stored, as a decimal string. */
  localSeq: string
  /** The identifier of this node's replication group. */
  replicationGroupId?: string
  /** The primary term that this node reports as current, as a decimal string. */
  primaryTerm?: string
  /** The identifier of the node that this node reports as the current primary. */
  currentPrimary?: string
  /** Whether this node reaches the coordinator, and whether it holds the primary authority. */
  coordinator?: {
    /** Whether this node reaches the coordinator. */
    connected: boolean
    /** Whether this node holds the primary authority. */
    authority: boolean
  }
  /** The state of this node's failover controller. */
  controller?: {
    /** Whether the controller is off, waiting, leading the group, or has lost its lease. */
    state: 'disabled' | 'standby' | 'active' | 'lost'
  }
  /** The replicas that the group counts as in sync. */
  inSyncReplicas?: string[]
  /** The replicas that have fallen behind. */
  laggingReplicas?: string[]
  /** How far this node has caught up with the primary. */
  syncState?: string
  /** The condition behind this node's health state. */
  healthReason: NodeHealthReason
  /** Whether this node serves reads now. */
  readAvailability?: 'available' | 'unavailable'
  /** Whether this node serves writes now. */
  writeAvailability?: 'available' | 'unavailable'
}

export function handleLiveness(): (res: HttpResponse, req: HttpRequest) => void {
  const payload = JSON.stringify({ status: 'ok' } satisfies LivenessResponse)

  return res => {
    endResponse(res, () => {
      res.writeStatus('200 OK').writeHeader('Content-Type', 'application/json').end(payload)
    })
  }
}

export function handleReadiness(
  sirannon: Sirannon,
  getReplicationStatus?: () => ReplicationStatusInfo | null,
): (res: HttpResponse, req: HttpRequest) => void {
  return res => {
    const dbs = sirannon.databases()
    const databases: DatabaseStatus[] = []
    let degraded = false

    for (const [id, db] of dbs) {
      const entry: DatabaseStatus = {
        id,
        readOnly: db.readOnly,
        closed: db.closed,
      }
      databases.push(entry)
      if (db.closed) degraded = true
    }

    const body: ReadinessResponse = {
      status: degraded ? 'degraded' : 'ok',
      databases,
    }

    if (getReplicationStatus) {
      const replStatus = getReplicationStatus()
      if (replStatus) {
        body.replication = {
          role: replStatus.role,
          writeForwarding: replStatus.writeForwarding,
          peers: replStatus.peers,
          localSeq: replStatus.localSeq.toString(),
          replicationGroupId: replStatus.replicationGroupId,
          primaryTerm: replStatus.primaryTerm?.toString(),
          currentPrimary: replStatus.currentPrimary,
          coordinator: replStatus.coordinator,
          controller: replStatus.controller,
          inSyncReplicas: replStatus.inSyncReplicas,
          laggingReplicas: replStatus.laggingReplicas,
          syncState: replStatus.syncState,
          healthReason: replStatus.health.reason,
          readAvailability: replStatus.health.canRead ? 'available' : 'unavailable',
          writeAvailability: replStatus.health.canWrite ? 'available' : 'unavailable',
        }
        body.status = readinessStatusForReplication(replStatus, body.status)
      }
    }

    const payload = JSON.stringify(body)
    endResponse(res, () => {
      res.writeStatus('200 OK').writeHeader('Content-Type', 'application/json').end(payload)
    })
  }
}

function readinessStatusForReplication(
  replication: ReplicationStatusInfo,
  current: ReadinessResponse['status'],
): ReadinessResponse['status'] {
  if (replication.health.state !== 'healthy') return replication.health.state
  return current
}
