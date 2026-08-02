import { randomUUID } from 'node:crypto'
import type { Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions } from '../../core/types.js'
import { SyncError, TopologyError } from '../errors.js'
import type { ForwardedTransactionResult } from '../types.js'
import {
  assertReadConcern,
  canAcceptLocalWrite,
  getCoordinatorMessageFields,
  getForwardingPrimaryPeerId,
  throwNotCurrentPrimary,
} from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'

function assertReadyPhase(engine: ReplicationEngine, activity: 'serve reads' | 'accept operations'): void {
  if (engine.syncState.phase !== 'ready') {
    throw new SyncError(`Node is in '${engine.syncState.phase}' phase and cannot ${activity}`)
  }
}

export async function query<T>(
  engine: ReplicationEngine,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<T[]> {
  assertReadyPhase(engine, 'serve reads')
  await assertReadConcern(engine, options?.readConcern?.level)
  return engine.database.query<T>(sql, params, options)
}

export async function execute(
  engine: ReplicationEngine,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<ExecuteResult> {
  assertReadyPhase(engine, 'accept operations')

  if (!(await canAcceptLocalWrite(engine))) {
    if (engine.config.writeForwarding) {
      const result = await forwardStatements(engine, [{ sql, params }], options)
      const first = result.results[0]
      if (!first) {
        return { changes: 0, lastInsertRowId: 0 }
      }
      return {
        changes: first.changes,
        lastInsertRowId:
          typeof first.lastInsertRowId === 'string' ? BigInt(first.lastInsertRowId) : first.lastInsertRowId,
      }
    }
    throwNotCurrentPrimary(engine)
  }

  return engine.localExecutor.executeLocally(sql, params, options)
}

export async function executeBatch(
  engine: ReplicationEngine,
  sql: string,
  paramsBatch: Params[],
  options?: QueryOptions,
): Promise<ExecuteResult[]> {
  assertReadyPhase(engine, 'accept operations')

  if (!(await canAcceptLocalWrite(engine))) {
    if (engine.config.writeForwarding) {
      const statements = paramsBatch.map(p => ({ sql, params: p }))
      const result = await forwardStatements(engine, statements, options)
      return result.results.map(r => ({
        changes: r.changes,
        lastInsertRowId: typeof r.lastInsertRowId === 'string' ? BigInt(r.lastInsertRowId) : Number(r.lastInsertRowId),
      }))
    }
    throwNotCurrentPrimary(engine)
  }

  const results: ExecuteResult[] = []
  for (const params of paramsBatch) {
    const r = await engine.localExecutor.executeLocally(sql, params, options)
    results.push(r)
  }
  return results
}

export async function transaction<T>(
  engine: ReplicationEngine,
  fn: (tx: Transaction) => Promise<T>,
  options?: QueryOptions,
): Promise<T> {
  assertReadyPhase(engine, 'accept operations')
  if (!(await canAcceptLocalWrite(engine))) {
    throw new TopologyError('This node cannot accept writes in transaction mode')
  }
  return engine.localExecutor.executeTransactionLocally(fn, options)
}

export async function forwardStatements(
  engine: ReplicationEngine,
  statements: Array<{ sql: string; params?: Params }>,
  _options?: QueryOptions,
): Promise<ForwardedTransactionResult> {
  if (await canAcceptLocalWrite(engine)) {
    return engine.localExecutor.executeForwardedLocally(statements)
  }

  const primaryPeerId = getForwardingPrimaryPeerId(engine)

  if (primaryPeerId === null) {
    throw new TopologyError('No primary node available for write forwarding')
  }

  return engine.config.transport.forward(primaryPeerId, {
    statements,
    requestId: randomUUID(),
    ...getCoordinatorMessageFields(engine),
  })
}
