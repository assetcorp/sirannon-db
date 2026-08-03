import type { Params } from './types.js'

/** One statement a registered operation runs, with its parameters bound.
 * @public
 */
export interface OperationStatement {
  /** The statement to run. */
  sql: string
  /** Parameters bound to that statement. */
  params?: Params
}

/** Values a caller passes when it invokes a registered operation by name.
 * @public
 */
export type OperationArguments = Record<string, unknown>

/** A read a caller invokes by name, so the server accepts no SQL from the network.
 * @public
 */
export interface ReadOperation<Identity = unknown> {
  /** Argument names this operation accepts from the caller. */
  args?: readonly string[]
  /** Arguments the server fills from the authenticated identity, so a caller cannot supply them. */
  fromIdentity?: Readonly<Record<string, keyof Identity & string>>
  /**
   * The columns every row of this read carries. Code generation emits a typed
   * row from it, and reads the columns from the statement only when the
   * operation takes no arguments, because arguments choose the statement.
   */
  columns?: readonly string[]
  /** Builds the statement this read runs for a given set of arguments. */
  statement(args: OperationArguments): OperationStatement
}

/** A write a caller invokes by name. The server runs every statement it returns in one transaction.
 * @public
 */
export interface WriteOperation<Identity = unknown> {
  /** Argument names this operation accepts from the caller. */
  args?: readonly string[]
  /** Arguments the server fills from the authenticated identity, so a caller cannot supply them. */
  fromIdentity?: Readonly<Record<string, keyof Identity & string>>
  /** Builds the statements this write runs for a given set of arguments. */
  statements(args: OperationArguments): OperationStatement | readonly OperationStatement[]
}

/** The reads and writes one database exposes by name.
 * @public
 */
export interface DatabaseOperations<Identity = unknown> {
  /** Reads callers may invoke, keyed by operation name. */
  reads?: Readonly<Record<string, ReadOperation<Identity>>>
  /** Writes callers may invoke, keyed by operation name. */
  writes?: Readonly<Record<string, WriteOperation<Identity>>>
}

/** Every database's registered operations, keyed by database identifier.
 * @public
 */
export type OperationRegistry<Identity = unknown> = Readonly<Record<string, DatabaseOperations<Identity>>>

/**
 * A named operation a remote caller invokes, carrying the argument and row
 * types of the registered operation. Only `name` exists at runtime; `types`
 * is never assigned and is present so both type parameters are inferable at
 * the call site. Code generation emits one reference per registered operation.
 *
 * @public
 */
export interface OperationRef<Args = OperationArguments, Row = Record<string, unknown>> {
  /** Name the server registered this operation under. */
  readonly name: string
  /** Present for type inference only, and never assigned at runtime. */
  readonly types?: { args: Args; row: Row }
}

/**
 * Builds a typed reference to a registered operation so that a call site infers its
 * argument and row types from the name alone.
 *
 * @param name - Name the server registered the operation under.
 * @returns A reference carrying that name and the two inferred types.
 *
 * @public
 */
export function operationRef<Args = OperationArguments, Row = Record<string, unknown>>(
  name: string,
): OperationRef<Args, Row> {
  return { name }
}

/**
 * Reads the operation name out of either a plain string or a typed reference.
 *
 * @param operation - The name itself, or a reference built by {@link operationRef}.
 * @returns The registered operation name.
 *
 * @public
 */
export function operationName(operation: string | OperationRef<never, never>): string {
  return typeof operation === 'string' ? operation : operation.name
}
