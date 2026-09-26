import type { Params } from './types.js'

/** One statement that a registered operation executes, with its parameters.
 * @public
 */
export interface OperationStatement {
  /** The statement to execute. */
  sql: string
  /** The parameters to bind to that statement. */
  params?: Params
}

/** The values that a caller passes when it invokes a registered operation by name.
 * @public
 */
export type OperationArguments = Record<string, unknown>

/** A read that a caller invokes by name, so that the caller sends no SQL over the network.
 * @public
 */
export interface ReadOperation<Identity = unknown> {
  /** The argument names that this operation accepts from the caller. */
  args?: readonly string[]
  /** The arguments that the server fills from the authenticated identity; a request that supplies one of them fails with `ARGUMENT_NOT_ALLOWED`. */
  fromIdentity?: Readonly<Record<string, keyof Identity & string>>
  /**
   * The column names of each row that this read returns. Code generation builds
   * a typed row from this list, and when the list is absent, it takes the
   * columns from the statement text, but only for an operation that takes no
   * arguments, because the statement text can change with the arguments.
   */
  columns?: readonly string[]
  /** Builds the statement that this read executes for a given set of arguments. */
  statement(args: OperationArguments): OperationStatement
}

/** A write that a caller invokes by name; the server executes every statement that it returns in one transaction.
 * @public
 */
export interface WriteOperation<Identity = unknown> {
  /** The argument names that this operation accepts from the caller. */
  args?: readonly string[]
  /** The arguments that the server fills from the authenticated identity; a request that supplies one of them fails with `ARGUMENT_NOT_ALLOWED`. */
  fromIdentity?: Readonly<Record<string, keyof Identity & string>>
  /** Builds the statements that this write executes for a given set of arguments. */
  statements(args: OperationArguments): OperationStatement | readonly OperationStatement[]
}

/** The reads and writes that one database exposes by name.
 * @public
 */
export interface DatabaseOperations<Identity = unknown> {
  /** The reads that callers may invoke, keyed by operation name. */
  reads?: Readonly<Record<string, ReadOperation<Identity>>>
  /** The writes that callers may invoke, keyed by operation name. */
  writes?: Readonly<Record<string, WriteOperation<Identity>>>
}

/** Every database's registered operations, keyed by database identifier.
 * @public
 */
export type OperationRegistry<Identity = unknown> = Readonly<Record<string, DatabaseOperations<Identity>>>

/**
 * A named operation that a remote caller invokes, typed with the argument and
 * row types of the registered operation. Only `name` exists at runtime, and
 * `types` stays unassigned so that the compiler can infer both type parameters
 * at the call site. Code generation emits one reference per registered
 * operation.
 *
 * @public
 */
export interface OperationRef<Args = OperationArguments, Row = Record<string, unknown>> {
  /** The name that the server registered this operation under. */
  readonly name: string
  /** A field for type inference only, which stays unassigned at runtime. */
  readonly types?: { args: Args; row: Row }
}

/**
 * Builds a typed reference to a registered operation, so that a call site infers its
 * argument and row types from the name alone.
 *
 * @param name - The name that the server registered the operation under.
 * @returns A reference with that name and the two inferred types.
 *
 * @public
 */
export function operationRef<Args = OperationArguments, Row = Record<string, unknown>>(
  name: string,
): OperationRef<Args, Row> {
  return { name }
}

/**
 * Returns the operation name from either a plain string or a typed reference.
 *
 * @param operation - The name itself, or a reference built by {@link operationRef}.
 * @returns The registered operation name.
 *
 * @public
 */
export function operationName(operation: string | OperationRef<never, never>): string {
  return typeof operation === 'string' ? operation : operation.name
}
