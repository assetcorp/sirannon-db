import type { Params } from './types.js'

export interface OperationStatement {
  sql: string
  params?: Params
}

export type OperationArguments = Record<string, unknown>

export interface ReadOperation<Identity = unknown> {
  args?: readonly string[]
  fromIdentity?: Readonly<Record<string, keyof Identity & string>>
  columns?: readonly string[]
  statement(args: OperationArguments): OperationStatement
}

export interface WriteOperation<Identity = unknown> {
  args?: readonly string[]
  fromIdentity?: Readonly<Record<string, keyof Identity & string>>
  statements(args: OperationArguments): OperationStatement | readonly OperationStatement[]
}

export interface DatabaseOperations<Identity = unknown> {
  reads?: Readonly<Record<string, ReadOperation<Identity>>>
  writes?: Readonly<Record<string, WriteOperation<Identity>>>
}

export type OperationRegistry<Identity = unknown> = Readonly<Record<string, DatabaseOperations<Identity>>>

/**
 * A named operation a remote caller invokes, carrying the argument and row
 * types of the registered operation. Only `name` exists at runtime; `types`
 * is never assigned and is present so both type parameters are inferable at
 * the call site. Code generation emits one reference per registered operation.
 */
export interface OperationRef<Args = OperationArguments, Row = Record<string, unknown>> {
  readonly name: string
  readonly types?: { args: Args; row: Row }
}

export function operationRef<Args = OperationArguments, Row = Record<string, unknown>>(
  name: string,
): OperationRef<Args, Row> {
  return { name }
}

export function operationName(operation: string | OperationRef<never, never>): string {
  return typeof operation === 'string' ? operation : operation.name
}
