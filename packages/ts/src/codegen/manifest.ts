import { type SqlToken, tokenizeSql } from '../core/live/sql-tokens.js'
import { findClauses, readSelectItems } from '../core/live/statement-clauses.js'
import type {
  OperationArguments,
  OperationRegistry,
  ReadOperation,
  WriteOperation,
} from '../core/operation-registry.js'
import { operationRegistryDigest } from '../server/operation-lookup.js'

export const OPERATION_MANIFEST_VERSION = 1

export interface OperationShape {
  args: string[]
  identityArgs: string[]
  columns: string[] | null
}

export interface DatabaseManifest {
  reads: Record<string, OperationShape>
  writes: Record<string, OperationShape>
}

export interface OperationManifest {
  version: number
  digest: string | undefined
  databases: Record<string, DatabaseManifest>
}

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
  const placeholders: OperationArguments = {}
  for (const name of [...args, ...identityArgs]) placeholders[name] = null

  try {
    return selectColumns(operation.statement(placeholders).sql)
  } catch {
    return null
  }
}

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
