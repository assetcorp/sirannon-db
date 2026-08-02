import type { HttpRequest, HttpResponse } from 'uWebSockets.js'
import type { Sirannon } from '../core/sirannon.js'
import type { NodeHealthReason, ReplicationStatusInfo } from '../core/types.js'

interface LivenessResponse {
  status: 'ok'
}

interface DatabaseStatus {
  id: string
  readOnly: boolean
  closed: boolean
}

interface ReadinessResponse {
  status: 'ok' | 'degraded' | 'failing_over' | 'unavailable' | 'repairing' | 'syncing'
  databases: DatabaseStatus[]
  replication?: {
    role: string
    writeForwarding: boolean
    peers: number
    localSeq: string
    replicationGroupId?: string
    primaryTerm?: string
    currentPrimary?: string
    coordinator?: {
      connected: boolean
      authority: boolean
    }
    controller?: {
      state: 'disabled' | 'standby' | 'active' | 'lost'
    }
    inSyncReplicas?: string[]
    laggingReplicas?: string[]
    syncState?: string
    healthReason: NodeHealthReason
    readAvailability?: 'available' | 'unavailable'
    writeAvailability?: 'available' | 'unavailable'
  }
}

export function handleLiveness(): (res: HttpResponse, req: HttpRequest) => void {
  const payload = JSON.stringify({ status: 'ok' } satisfies LivenessResponse)

  return res => {
    res.cork(() => {
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
    res.cork(() => {
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
