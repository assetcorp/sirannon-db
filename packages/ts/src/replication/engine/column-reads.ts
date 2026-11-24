import type { Params, QueryWithColumnsOptions, QueryWithColumnsResult } from '../../core/types.js'
import { assertReadConcern } from './coordinator-authority.js'
import { assertReadyPhase } from './data-api.js'
import type { ReplicationEngine } from './engine.js'

/**
 * Executes a read on this node and returns each column's details with each row as an array, once the node has
 * finished its first sync and can meet the read concern.
 *
 * @param engine - The replication engine of this node.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @param options - The read concern and the two limits.
 * @returns The columns, the rows, and the limit that stopped the read.
 * @internal
 */
export async function queryWithColumns(
  engine: ReplicationEngine,
  sql: string,
  params?: Params,
  options?: QueryWithColumnsOptions,
): Promise<QueryWithColumnsResult> {
  assertReadyPhase(engine, 'serve reads')
  await assertReadConcern(engine, options?.readConcern?.level)
  return engine.database.queryWithColumns(sql, params, options)
}
