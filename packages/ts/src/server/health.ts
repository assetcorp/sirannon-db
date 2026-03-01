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
}

/**
 * Returns a handler that responds with a simple `{ status: 'ok' }` payload.
 * This endpoint confirms the process is running and able to serve requests.
 */
export function handleLiveness(): (res: HttpResponse, req: HttpRequest) => void {
  const payload = JSON.stringify({ status: 'ok' } satisfies LivenessResponse)

  return res => {
    res.cork(() => {
      res.writeStatus('200 OK').writeHeader('Content-Type', 'application/json').end(payload)
    })
  }
}

/**
 * Returns a handler that responds with the readiness status of every
 * registered database. If any database is closed, the overall status
 * is `degraded` (still 200, so load balancers can decide based on the
 * body).
 */
export function handleReadiness(sirannon: Sirannon): (res: HttpResponse, req: HttpRequest) => void {
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

    const payload = JSON.stringify(body)
    res.cork(() => {
      res.writeStatus('200 OK').writeHeader('Content-Type', 'application/json').end(payload)
    })
  }
}
