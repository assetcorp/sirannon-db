import { SirannonClient } from '../../../client/index.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { DatabaseOperations, Params } from '../../../core/types.js'
import { postJson, RawSocket } from '../lib/served-registry.js'
import { INSERT_ORDER, SCAN_BY_CUSTOMER, SET_TOTAL_BY_ID } from './metered-registry.js'

export interface Answer {
  ok: boolean
  text: string
}

export interface Statement {
  sql: string
  params?: Params
}

export interface Caller {
  readonly label: string
  query(sql: string, params?: Params): Promise<Answer>
  execute(sql: string, params?: Params): Promise<Answer>
  transaction(statements: Statement[]): Promise<Answer>
  batch(sql: string, paramsBatch: Params[]): Promise<Answer>
  load(sql: string, paramsBatch: Params[]): Promise<Answer>
  namedQuery(name: string, args: Record<string, unknown>): Promise<Answer>
  namedExecute(name: string, args: Record<string, unknown>): Promise<Answer>
  close(): void
}

export const ORDERS_OF_CUSTOMER = 'ordersOfCustomer'
export const PLACE_ORDER = 'placeOrder'
export const PLACE_AND_REPRICE = 'placeAndReprice'

export const METERED_OPERATIONS: DatabaseOperations = {
  reads: {
    [ORDERS_OF_CUSTOMER]: {
      args: ['customer'],
      statement: args => ({ sql: SCAN_BY_CUSTOMER, params: [args.customer] }),
    },
  },
  writes: {
    [PLACE_ORDER]: {
      args: ['customer', 'total'],
      statements: args => ({ sql: INSERT_ORDER, params: [args.customer, args.total] }),
    },
    [PLACE_AND_REPRICE]: {
      args: ['customer', 'id'],
      statements: args => [
        { sql: INSERT_ORDER, params: [args.customer, 1] },
        { sql: SET_TOTAL_BY_ID, params: [9, args.id] },
      ],
    },
  },
}

function wireText(value: unknown): string {
  return JSON.stringify(value, (_, item) => (typeof item === 'bigint' ? item.toString() : item)) ?? ''
}

export function httpCaller(base: string, databaseId: string): Caller {
  const post = async (path: string, body: unknown): Promise<Answer> => {
    const answer = await postJson(base, `/db/${databaseId}${path}`, body)
    return { ok: answer.status === 200, text: answer.text }
  }
  return {
    label: 'raw HTTP',
    query: (sql, params) => post('/query', { sql, params }),
    execute: (sql, params) => post('/execute', { sql, params }),
    transaction: statements => post('/transaction', { statements }),
    batch: (sql, paramsBatch) => post('/batch', { sql, paramsBatch }),
    load: (sql, paramsBatch) => post('/load', { sql, paramsBatch }),
    namedQuery: (name, args) => post(`/query/${encodeURIComponent(name)}`, { args }),
    namedExecute: (name, args) => post(`/execute/${encodeURIComponent(name)}`, { args }),
    close: () => {},
  }
}

export async function webSocketCaller(wsBase: string, databaseId: string): Promise<Caller> {
  const socket = await RawSocket.open(`${wsBase}/db/${databaseId}`)
  let next = 0
  const send = async (message: Record<string, unknown>): Promise<Answer> => {
    const reply = await socket.request({ ...message, id: `metered-${next++}` })
    return { ok: reply.type === 'result', text: JSON.stringify(reply) }
  }
  return {
    label: 'raw WebSocket',
    query: (sql, params) => send({ type: 'query', sql, params }),
    execute: (sql, params) => send({ type: 'execute', sql, params }),
    transaction: statements => send({ type: 'transaction', statements }),
    batch: (sql, paramsBatch) => send({ type: 'batch', sql, paramsBatch }),
    load: (sql, paramsBatch) => send({ type: 'load', sql, paramsBatch }),
    namedQuery: (name, args) => send({ type: 'query', name, args }),
    namedExecute: (name, args) => send({ type: 'execute', name, args }),
    close: () => socket.close(),
  }
}

export function clientCaller(base: string, databaseId: string, transport: 'http' | 'websocket'): Caller {
  const client = new SirannonClient(base, { transport, autoReconnect: false })
  const db = client.database(databaseId)
  const settle = async (call: () => Promise<unknown>): Promise<Answer> => {
    try {
      return { ok: true, text: wireText(await call()) }
    } catch (err) {
      const failure = err as { code?: string; message?: string }
      return { ok: false, text: wireText({ code: failure.code, message: failure.message }) }
    }
  }
  return {
    label: `the published client over ${transport}`,
    query: (sql, params) => settle(() => db.query(sql, params)),
    execute: (sql, params) => settle(() => db.execute(sql, params)),
    transaction: statements => settle(() => db.transaction(statements)),
    batch: (sql, paramsBatch) => settle(() => db.batch(sql, paramsBatch)),
    load: (sql, paramsBatch) => settle(() => db.load(sql, paramsBatch)),
    namedQuery: (name, args) => settle(() => db.query(operationRef(name), args)),
    namedExecute: (name, args) => settle(() => db.execute(operationRef(name), args)),
    close: () => client.close(),
  }
}
