import { expect } from 'vitest'
import { SirannonClient } from '../../client/index.js'
import { type GateEnvironment, GROUP_ID, httpPortFor, serverBaseUrlFor } from './gate-environment.js'
import type { FailoverNodeProcess } from './node-process.js'

export async function executeMajority(node: FailoverNodeProcess, id: number, note: string): Promise<void> {
  await node.execute(
    'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
    [id, node.config.nodeId, id, note],
    { writeConcern: { level: 'majority', timeoutMs: 15_000 } },
  )
}

export async function executePublicHttp(environment: GateEnvironment, nodeId: string, sql: string): Promise<void> {
  const response = await fetch(`${serverBaseUrlFor(environment, nodeId)}/db/${GROUP_ID}/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sql,
      writeConcern: { level: 'majority', timeoutMs: 15_000 },
    }),
  })
  if (!response.ok) {
    const body = await response.text()
    throw new Error(`Public HTTP execute on ${nodeId} returned ${response.status}: ${body}`)
  }
}

export async function expectPublicHttpRejectsWith(
  environment: GateEnvironment,
  nodeId: string,
  sql: string,
  codes: string[],
): Promise<void> {
  const response = await fetch(`${serverBaseUrlFor(environment, nodeId)}/db/${GROUP_ID}/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sql,
      writeConcern: { level: 'majority', timeoutMs: 15_000 },
    }),
  })
  const data = (await response.json()) as { error?: { code?: string } }
  expect(response.ok).toBe(false)
  expect(codes).toContain(data.error?.code)
}

export async function executePublicClient(
  environment: GateEnvironment,
  starterNodeId: string,
  sql: string,
): Promise<void> {
  const client = new SirannonClient({
    endpoints: [serverBaseUrlFor(environment, starterNodeId)],
    discovery: 'coordinator',
    transport: 'http',
  })
  try {
    const db = client.database(GROUP_ID)
    await db.execute(sql)
  } finally {
    client.close()
  }
}

export async function executePublicWebSocket(environment: GateEnvironment, nodeId: string, sql: string): Promise<void> {
  const wsUrl = `ws://127.0.0.1:${httpPortFor(environment, nodeId)}/db/${GROUP_ID}`
  const ws = new WebSocket(wsUrl)
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
    ws.send(
      JSON.stringify({
        id: requestId,
        type: 'execute',
        sql,
      }),
    )
  })
}
