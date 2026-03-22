import type { HttpRequest, HttpResponse } from 'uWebSockets.js'
import type { Sirannon } from '../core/sirannon.js'

interface LivenessResponse {
  status: 'ok'
}

interface DatabaseStatus {
  id: string
  readOnly: boolean
  closed: boolean
}

interface ReadinessResponse {
  status: 'ok' | 'degraded'
  databases: DatabaseStatus[]
  replication?: {
    role: string
    writeForwarding: boolean
    peers: number
    localSeq: string
  }
}

export interface ReplicationHealthInfo {
  role: string
  writeForwarding: boolean
  peers: number
  localSeq: bigint
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
  getReplicationStatus?: () => ReplicationHealthInfo | null,
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
        }
      }
    }

    const payload = JSON.stringify(body)
    res.cork(() => {
      res.writeStatus('200 OK').writeHeader('Content-Type', 'application/json').end(payload)
    })
  }
}
