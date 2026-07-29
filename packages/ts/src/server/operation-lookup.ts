import type {
  DatabaseOperations,
  OperationArguments,
  OperationRegistry,
  OperationStatement,
  ReadOperation,
  WriteOperation,
} from '../core/operation-registry.js'
import { sha256Hex } from '../core/sync/sha256.js'

const DIGEST_FIELD_SEPARATOR = '\u0000'

export interface OperationRefusal {
  status: number
  code: string
  message: string
}

export type ArgumentResolution = { ok: true; value: OperationArguments } | { ok: false; refusal: OperationRefusal }

function unknownOperation(name: string, databaseId: string): OperationRefusal {
  return {
    status: 404,
    code: 'UNKNOWN_QUERY',
    message: `No operation named '${name}' is registered for database '${databaseId}'`,
  }
}

export function findRead<I>(
  registry: OperationRegistry<I> | undefined,
  databaseId: string,
  name: string,
): ReadOperation<I> | OperationRefusal {
  const operation = registry?.[databaseId]?.reads?.[name]
  return operation ?? unknownOperation(name, databaseId)
}

export function findWrite<I>(
  registry: OperationRegistry<I> | undefined,
  databaseId: string,
  name: string,
): WriteOperation<I> | OperationRefusal {
  const operation = registry?.[databaseId]?.writes?.[name]
  return operation ?? unknownOperation(name, databaseId)
}

export function isRefusal(value: object): value is OperationRefusal {
  return 'status' in value && 'code' in value && 'message' in value
}

export function resolveArguments<I>(
  operation: { args?: readonly string[]; fromIdentity?: Readonly<Record<string, keyof I & string>> },
  supplied: OperationArguments | undefined,
  identity: I | undefined,
): ArgumentResolution {
  const declared = operation.args ?? []
  const identityFields = operation.fromIdentity ?? {}
  const resolved: OperationArguments = {}

  for (const name of Object.keys(supplied ?? {})) {
    if (Object.hasOwn(identityFields, name)) {
      return {
        ok: false,
        refusal: {
          status: 400,
          code: 'ARGUMENT_NOT_ALLOWED',
          message: `Argument '${name}' is filled from the authenticated identity and cannot be supplied by the caller`,
        },
      }
    }
    if (!declared.includes(name)) {
      return {
        ok: false,
        refusal: {
          status: 400,
          code: 'ARGUMENT_NOT_ALLOWED',
          message: `Argument '${name}' is not declared by this operation`,
        },
      }
    }
  }

  for (const name of declared) {
    if (supplied === undefined || !Object.hasOwn(supplied, name)) {
      return {
        ok: false,
        refusal: { status: 400, code: 'MISSING_ARGUMENT', message: `Argument '${name}' is required` },
      }
    }
    resolved[name] = supplied[name]
  }

  for (const [name, field] of Object.entries(identityFields)) {
    const source = identity as Record<string, unknown> | undefined | null
    const value = source === undefined || source === null ? undefined : source[field]
    if (value === undefined) {
      return {
        ok: false,
        refusal: {
          status: 401,
          code: 'IDENTITY_REQUIRED',
          message: `Argument '${name}' is filled from identity field '${String(field)}', which this request does not carry`,
        },
      }
    }
    resolved[name] = value
  }

  return { ok: true, value: resolved }
}

export type OperationKind = 'read' | 'write'

export type ResolvedOperation =
  | { ok: true; statements: readonly OperationStatement[] }
  | { ok: false; refusal: OperationRefusal }

export interface OperationSource {
  digest: string | undefined
  resolve(
    kind: OperationKind,
    databaseId: string,
    name: string,
    args: OperationArguments | undefined,
    identity: unknown,
  ): ResolvedOperation
}

export function createOperationSource<I>(registry: OperationRegistry<I> | undefined): OperationSource {
  return {
    digest: operationRegistryDigest(registry),
    resolve: (kind, databaseId, name, supplied, identity) => {
      const operation = kind === 'read' ? findRead(registry, databaseId, name) : findWrite(registry, databaseId, name)
      if (isRefusal(operation)) return { ok: false, refusal: operation }

      const args = resolveArguments<I>(operation, supplied, identity as I | undefined)
      if (!args.ok) return { ok: false, refusal: args.refusal }

      if (kind === 'read') {
        return { ok: true, statements: [(operation as ReadOperation<I>).statement(args.value)] }
      }

      const produced = (operation as WriteOperation<I>).statements(args.value)
      const statements = Array.isArray(produced) ? produced : [produced as OperationStatement]
      if (statements.length === 0) {
        return {
          ok: false,
          refusal: { status: 500, code: 'INTERNAL_ERROR', message: `Operation '${name}' produced no statements` },
        }
      }
      return { ok: true, statements }
    },
  }
}

function operationLine(databaseId: string, kind: string, name: string, operation: object): string {
  const declared = [...((operation as { args?: readonly string[] }).args ?? [])].sort()
  const identityFields = Object.keys(
    (operation as { fromIdentity?: Readonly<Record<string, string>> }).fromIdentity ?? {},
  ).sort()
  return [databaseId, kind, name, declared.join(','), identityFields.join(',')].join(DIGEST_FIELD_SEPARATOR)
}

export function operationRegistryDigest<I>(registry: OperationRegistry<I> | undefined): string | undefined {
  if (registry === undefined) return undefined

  const lines: string[] = []
  for (const databaseId of Object.keys(registry).sort()) {
    const operations: DatabaseOperations<I> = registry[databaseId] ?? {}
    for (const name of Object.keys(operations.reads ?? {}).sort()) {
      const read = operations.reads?.[name]
      if (read) lines.push(operationLine(databaseId, 'read', name, read))
    }
    for (const name of Object.keys(operations.writes ?? {}).sort()) {
      const write = operations.writes?.[name]
      if (write) lines.push(operationLine(databaseId, 'write', name, write))
    }
  }
  return sha256Hex(lines.join('\n'))
}
