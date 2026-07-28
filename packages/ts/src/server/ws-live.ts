import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { LiveQuery, LiveUpdate } from '../core/live/types.js'
import type { Params } from '../core/types.js'
import type { OperationSource } from './operation-lookup.js'
import type { WSLiveMessage, WSLiveOp } from './protocol.js'
import type { WSConnection } from './ws-connection.js'
import type { ConnectionState } from './ws-handler.js'
import { readArguments } from './ws-named.js'

const LIVE_ONLY_FIELDS = ['table', 'tables', 'filter', 'sinceSeq', 'epoch', 'deviceId', 'schemaVersion'] as const

export interface WSLiveDeps {
  operations: OperationSource
  sendSubscribedRows(conn: WSConnection, id: string, rows: unknown[]): void
  sendLive(conn: WSConnection, message: WSLiveMessage): void
  sendError(conn: WSConnection, id: string, code: string, message: string): void
  sendSirannonError(conn: WSConnection, id: string, err: unknown): void
}

export async function handleLiveSubscribeMessage(
  deps: WSLiveDeps,
  conn: WSConnection,
  state: ConnectionState,
  msg: Record<string, unknown>,
  id: string,
  name: string,
): Promise<void> {
  const rejected = LIVE_ONLY_FIELDS.find(field => msg[field] !== undefined)
  if (rejected !== undefined) {
    deps.sendError(
      conn,
      id,
      'INVALID_MESSAGE',
      `A subscription naming a registered read carries no "${rejected}"; the server holds the result`,
    )
    return
  }

  if (state.subscriptions.has(id)) {
    deps.sendError(conn, id, 'DUPLICATE_SUBSCRIPTION', `Subscription '${id}' already exists on this connection`)
    return
  }

  if (state.database.readOnly) {
    deps.sendError(conn, id, 'READ_ONLY', 'Live queries are not available on read-only databases')
    return
  }

  if (state.database.path === ':memory:') {
    deps.sendError(conn, id, 'CDC_UNSUPPORTED', 'Live queries require file-based databases')
    return
  }

  if (msg.registryDigest !== undefined) {
    if (typeof msg.registryDigest !== 'string') {
      deps.sendError(conn, id, 'INVALID_MESSAGE', '"registryDigest" must be a string')
      return
    }
    if (msg.registryDigest !== deps.operations.digest) {
      deps.sendError(
        conn,
        id,
        'REGISTRY_MISMATCH',
        'This server runs a different operation registry than the one the client generated against; re-read GET /capabilities',
      )
      return
    }
  }

  const args = readArguments(msg.args)
  if (!args.ok) {
    deps.sendError(conn, id, 'INVALID_MESSAGE', args.message)
    return
  }

  let sql: string
  let params: Params | undefined
  try {
    const resolved = deps.operations.resolve('read', state.databaseId, name, args.value, state.identity)
    if (!resolved.ok) {
      deps.sendError(conn, id, resolved.refusal.code, resolved.refusal.message)
      return
    }
    sql = resolved.statements[0].sql
    params = resolved.statements[0].params
  } catch (err) {
    deps.sendSirannonError(conn, id, err)
    return
  }

  let cancelled = false
  state.subscriptions.set(id, {
    unsubscribe: () => {
      cancelled = true
    },
  })

  let query: LiveQuery<Record<string, unknown>>
  try {
    query = await state.database.live(sql, params)
  } catch (err) {
    state.subscriptions.delete(id)
    if (!cancelled) deps.sendSirannonError(conn, id, err)
    return
  }

  if (cancelled) {
    await query.close().catch(() => {})
    return
  }

  const stop = query.subscribe(update => {
    deliver(deps, conn, id, query, update)
  })

  state.subscriptions.set(id, {
    unsubscribe: () => {
      stop()
      query.close().catch(() => {})
    },
  })

  const opened = query.getState()
  if (opened.status !== 'ready') {
    deps.sendError(conn, id, 'INTERNAL_ERROR', 'The live query produced no first result')
    return
  }
  deps.sendSubscribedRows(conn, id, encodeRows(opened.rows))
}

function deliver(
  deps: WSLiveDeps,
  conn: WSConnection,
  id: string,
  query: LiveQuery<Record<string, unknown>>,
  update: LiveUpdate<Record<string, unknown>>,
): void {
  if (update.kind === 'ops') {
    deps.sendLive(conn, { type: 'live', id, ops: update.ops.map(encodeOp) })
    return
  }
  if (update.kind === 'revalidating') {
    deps.sendLive(conn, { type: 'live', id, revalidating: true })
    return
  }

  const state = query.getState()
  if (state.status === 'ready') {
    deps.sendLive(conn, { type: 'live', id, rows: encodeRows(state.rows) })
    return
  }
  if (state.status === 'error') {
    deps.sendError(conn, id, 'CDC_ERROR', state.error.message)
  }
}

function encodeRows(rows: readonly Record<string, unknown>[]): unknown[] {
  return rows.map(row => encodeTaggedValues(row))
}

function encodeOp(op: { op: string; index: number; row?: Record<string, unknown> }): WSLiveOp {
  if (op.op === 'delete') return { op: 'delete', index: op.index }
  return {
    op: op.op === 'insert' ? 'insert' : 'update',
    index: op.index,
    row: encodeTaggedValues(op.row),
  }
}
