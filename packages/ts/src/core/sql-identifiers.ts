/**
 * The three names through which SQLite lets a statement read a table's `rowid` wherever the table declares no column
 * of that name.
 *
 * @internal
 */
export const ROWID_ALIASES: readonly string[] = ['rowid', 'oid', '_rowid_']

/**
 * Returns an identifier with every double quote doubled, which SQLite reads back as the same identifier once the
 * result is wrapped in double quotes.
 *
 * @param name - The identifier.
 * @returns The escaped identifier, without the surrounding quotes.
 * @internal
 */
export function escId(name: string): string {
  return name.replace(/"/g, '""')
}
