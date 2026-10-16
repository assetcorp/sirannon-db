import { ConflictError } from '../errors.js'
import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'

/**
 * Returns the outcome that the table's resolver chooses for one conflicting row.
 *
 * A {@link ConflictError} from the resolver propagates unchanged. Any other
 * error becomes a {@link ConflictError} that names the table and the row and
 * keeps the original error as its cause.
 *
 * @param resolver - One resolver for every table, or a function that returns the resolver for a table.
 * @param ctx - The local and incoming versions of the row.
 * @returns The resolver's outcome.
 * @internal
 */
export async function resolveConflict(
  resolver: ConflictResolver | ((table: string) => ConflictResolver),
  ctx: ConflictContext,
): Promise<ConflictResolution> {
  try {
    const changeResolver = typeof resolver === 'function' ? resolver(ctx.table) : resolver
    return await changeResolver.resolve(ctx)
  } catch (err: unknown) {
    if (err instanceof ConflictError) throw err
    const reason = err instanceof Error ? err.message : String(err)
    throw new ConflictError(
      `Conflict resolution failed for table '${ctx.table}' row '${ctx.rowId}': ${reason}`,
      ctx.table,
      ctx.rowId,
      err,
    )
  }
}
