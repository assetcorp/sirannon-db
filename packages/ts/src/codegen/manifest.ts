import { type SqlToken, tokenizeSql } from '../core/live/sql-tokens.js'
import { findClauses, readSelectItems } from '../core/live/statement-clauses.js'
import type { OperationRegistry, ReadOperation, WriteOperation } from '../core/operation-registry.js'
import { operationRegistryDigest } from '../server/operation-lookup.js'

/**
 * @public
 *
 * Version of the manifest format the generator writes.
 */
export const OPERATION_MANIFEST_VERSION = 1

/**
 * @public
 *
 * The arguments and columns of one registered operation, as code generation reads them.
 */
export interface OperationShape {
  /**
   * Argument names the caller supplies.
   */
  args: string[]
  /**
   * Argument names the server fills from the authenticated identity.
   */
  identityArgs: string[]
  /**
   * Columns every row of a read carries, or null when the generator cannot tell.
   */
  columns: string[] | null
}

/**
 * @public
 *
 * The reads and writes one database exposes by name.
 */
export interface DatabaseManifest {
  /**
   * Registered reads, keyed by operation name.
   */
  reads: Record<string, OperationShape>
  /**
   * Registered writes, keyed by operation name.
   */
  writes: Record<string, OperationShape>
}

/**
 * @public
 *
 * Every database's registered operations, in the form code generation reads.
 */
export interface OperationManifest {
  /**
   * Version of the manifest format.
   */
  version: number
  /**
   * Digest of the registry, which a client compares against the server's.
   */
  digest: string | undefined
  /**
   * One manifest per database, keyed by database identifier.
   */
  databases: Record<string, DatabaseManifest>
}

/**
 * @public
 *
 * Reads a registry and describes each operation's arguments and columns.
 *
 * @param registry - The registered operations to describe.
 * @returns The manifest code generation renders types from.
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
 * @public
 *
 * Reads the column names a SELECT statement produces.
 *
 * @param sql - The statement to inspect.
 * @returns The column names, or null when the statement's columns cannot be read from its text.
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
