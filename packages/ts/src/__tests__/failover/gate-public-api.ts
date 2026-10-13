import { expect } from 'vitest'
import { TopologyAwareClient } from '../../client/topology.js'
import { type GateEnvironment, GROUP_ID, httpPortFor, serverBaseUrlFor } from './gate-environment.js'
import type { FailoverNodeProcess } from './node-process.js'
import type { ItemRow } from './sqlite-oracle.js'

export interface HttpAnswer {
  status: number
  body: {
    changes?: number
    error?: { code?: string; message?: string; details?: Record<string, unknown> }
    [key: string]: unknown
  }
}

export function itemRow(id: number, owner: string, note: string): ItemRow {
  return { id, owner, value: id, note }
}

export async function writeItemMajority(node: FailoverNodeProcess, row: ItemRow, timeoutMs = 15_000): Promise<void> {
  await node.execute(
    'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
    [row.id, row.owner, row.value, row.note],
    { writeConcern: { level: 'majority', timeoutMs } },
  )
}

export async function postSql(
  environment: GateEnvironment,
  nodeId: string,
  route: 'execute' | 'query' | 'transaction',
  body: Record<string, unknown>,
): Promise<HttpAnswer> {
  const response = await fetch(`${serverBaseUrlFor(environment, nodeId)}/db/${GROUP_ID}/${route}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(20_000),
  })
  const text = await response.text()
  return { status: response.status, body: text === '' ? {} : (JSON.parse(text) as HttpAnswer['body']) }
}

export async function executePublicHttp(environment: GateEnvironment, nodeId: string, sql: string): Promise<void> {
  const answer = await postSql(environment, nodeId, 'execute', {
    sql,
    writeConcern: { level: 'majority', timeoutMs: 15_000 },
  })
  if (answer.status !== 200) {
    throw new Error(`Public HTTP execute on ${nodeId} returned ${answer.status}: ${JSON.stringify(answer.body)}`)
  }
}

export async function expectPublicHttpRejectsWith(
  environment: GateEnvironment,
  nodeId: string,
  sql: string,
  codes: readonly string[],
): Promise<HttpAnswer> {
  const answer = await postSql(environment, nodeId, 'execute', {
    sql,
    writeConcern: { level: 'majority', timeoutMs: 15_000 },
  })
  expect(answer.status).toBeGreaterThanOrEqual(400)
  expect(codes).toContain(answer.body.error?.code)
  return answer
}

export async function executePublicClient(
  environment: GateEnvironment,
  starterNodeId: string,
  sql: string,
): Promise<void> {
  const client = new TopologyAwareClient({
    endpoints: [serverBaseUrlFor(environment, starterNodeId)],
    discovery: 'coordinator',
    transport: 'http',
  })
  try {
    await client.database(GROUP_ID).execute(sql)
  } finally {
    client.close()
  }
}

export async function executePublicWebSocket(environment: GateEnvironment, nodeId: string, sql: string): Promise<void> {
  const ws = new WebSocket(`ws://127.0.0.1:${httpPortFor(environment, nodeId)}/db/${GROUP_ID}`)
  try {
    await waitForWebSocketOpen(ws)
    const result = await sendWebSocketExecute(ws, sql)
    expect(result.changes).toBe(1)
  } finally {
    ws.close()
  }
}

function waitForWebSocketOpen(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Timed out waiting for public WebSocket open')), 10_000)
    ws.addEventListener('open', () => {
      clearTimeout(timeout)
      resolve()
    })
    ws.addEventListener('error', () => {
      clearTimeout(timeout)
      reject(new Error('Public WebSocket failed to open'))
    })
  })
}

function sendWebSocketExecute(ws: WebSocket, sql: string): Promise<{ changes: number }> {
  return new Promise((resolve, reject) => {
    const requestId = `public-ws-${Date.now()}`
    const timeout = setTimeout(() => reject(new Error('Timed out waiting for public WebSocket execute')), 20_000)
    const onMessage = (event: MessageEvent) => {
      const data = JSON.parse(String(event.data)) as {
        type: string
        id: string
        data?: { changes?: number }
        error?: { code?: string; message?: string }
      }
      if (data.id !== requestId) return
      clearTimeout(timeout)
      ws.removeEventListener('message', onMessage)
      if (data.type === 'error') {
        reject(new Error(`Public WebSocket execute failed with ${data.error?.code}: ${data.error?.message}`))
        return
      }
      resolve({ changes: Number(data.data?.changes ?? 0) })
    }
    ws.addEventListener('message', onMessage)
    ws.send(JSON.stringify({ id: requestId, type: 'execute', sql }))
  })
}
