import { type SqlToken, tokenizeSql } from '../core/live/sql-tokens.js'
import { findClauses, readSelectItems } from '../core/live/statement-clauses.js'
import type { OperationRegistry, ReadOperation, WriteOperation } from '../core/operation-registry.js'
import { operationRegistryDigest } from '../server/operation-lookup.js'

/**
 * The version of the manifest format that the generator writes.
 *
 * @public
 */
export const OPERATION_MANIFEST_VERSION = 1

/**
 * Describes the arguments and result columns of one registered operation for code generation.
 *
 * @public
 */
export interface OperationShape {
  /**
   * The names of the arguments that the caller supplies.
   */
  args: string[]
  /**
   * The names of the arguments that the server sets from the authenticated identity.
   */
  identityArgs: string[]
  /**
   * The column names in every row of a read, or `null` for a write and for a read whose columns the generator cannot derive.
   */
  columns: string[] | null
}

/**
 * Describes the named reads and writes that the registry holds for one database.
 *
 * @public
 */
export interface DatabaseManifest {
  /**
   * The registered reads, keyed by operation name.
   */
  reads: Record<string, OperationShape>
  /**
   * The registered writes, keyed by operation name.
   */
  writes: Record<string, OperationShape>
}

/**
 * Describes every database's registered operations for code generation.
 *
 * @public
 */
export interface OperationManifest {
  /**
   * The version of the manifest format.
   */
  version: number
  /**
   * The registry digest, which a client can compare with the digest that the server announces.
   */
  digest: string | undefined
  /**
   * One manifest for each database, keyed by database ID.
   */
  databases: Record<string, DatabaseManifest>
}

/**
 * Returns a manifest that describes the arguments and result columns of every operation in a registry.
 *
 * @param registry - The registered operations to describe.
 * @returns The manifest from which code generation renders types.
 *
 * @public
 */
export function buildOperationManifest<I>(registry: OperationRegistry<I>): OperationManifest {
  const databases: Record<string, DatabaseManifest> = {}

  for (const databaseId of Object.keys(registry).sort()) {
    const operations = registry[databaseId] ?? {}
    const reads: Record<string, OperationShape> = {}
    const writes: Record<string, OperationShape> = {}

    for (const name of Object.keys(operations.reads ?? {}).sort()) {
      const read = operations.reads?.[name]
      if (read !== undefined) reads[name] = readShape(read)
    }
    for (const name of Object.keys(operations.writes ?? {}).sort()) {
      const write = operations.writes?.[name]
      if (write !== undefined) writes[name] = writeShape(write)
    }

    databases[databaseId] = { reads, writes }
  }

  return { version: OPERATION_MANIFEST_VERSION, digest: operationRegistryDigest(registry), databases }
}

function readShape<I>(operation: ReadOperation<I>): OperationShape {
  const args = [...(operation.args ?? [])]
  const identityArgs = Object.keys(operation.fromIdentity ?? {})
  return { args, identityArgs, columns: statementColumns(operation, args, identityArgs) }
}

function writeShape<I>(operation: WriteOperation<I>): OperationShape {
  return { args: [...(operation.args ?? [])], identityArgs: Object.keys(operation.fromIdentity ?? {}), columns: null }
}

function statementColumns<I>(operation: ReadOperation<I>, args: string[], identityArgs: string[]): string[] | null {
  if (operation.columns !== undefined) return [...operation.columns]
  if (args.length > 0 || identityArgs.length > 0) return null

  try {
    return selectColumns(operation.statement({}).sql)
  } catch {
    return null
  }
}

/**
 * Returns the column names that a `SELECT` statement produces.
 *
 * @param sql - The statement to inspect.
 * @returns The column names, or `null` when the parser cannot derive every column name from the statement's text.
 *
 * @public
 */
export function selectColumns(sql: string): string[] | null {
  const tokens = withoutTrailingSemicolon(tokenizeSql(sql))
  if (tokens.length === 0 || tokens[0].lower !== 'select' || tokens[0].quoted) return null

  const items = readSelectItems(sql, tokens, findClauses(sql, tokens))
  const names: string[] = []
  for (const item of items) {
    if (item.star || item.alias === null) return null
    names.push(item.alias)
  }
  return names.length === 0 ? null : names
}

function withoutTrailingSemicolon(tokens: SqlToken[]): SqlToken[] {
  const last = tokens[tokens.length - 1]
  if (last !== undefined && last.kind === 'punct' && last.value === ';') return tokens.slice(0, -1)
  return tokens
}
