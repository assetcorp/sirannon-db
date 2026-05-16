import type { SQLiteConnection, SQLiteDriver } from './driver/types.js'
import { ExtensionError } from './errors.js'

/**
 * Validates a filesystem path supplied to `Database.loadExtension`, then
 * issues the `SELECT load_extension(...)` call on the writer connection.
 *
 * Validation rejects:
 * - empty paths or paths carrying embedded null bytes (the path is
 *   eventually interpolated into SQL).
 * - paths containing ASCII control characters; SQLite would interpret
 *   most of them and the operator log would otherwise carry unprintable
 *   characters.
 * - paths whose segments include the `..` directory-traversal token; the
 *   resolved path is what the driver opens, but we reject untrusted
 *   inputs before they reach the filesystem so configuration mistakes
 *   surface loudly rather than silently loading an arbitrary `.so`.
 *
 * Single quotes in the resolved path are escaped by doubling them before
 * being interpolated into the SQL. `load_extension` is a SQLite function
 * call, so we cannot use parameter binding directly.
 */
export async function loadExtension(
  driver: SQLiteDriver,
  writer: SQLiteConnection,
  extensionPath: string,
): Promise<void> {
  if (!driver.capabilities.extensions) {
    throw new ExtensionError(extensionPath, 'Extensions are not supported by the current driver')
  }

  if (!extensionPath || extensionPath.includes('\0')) {
    throw new ExtensionError(extensionPath || '', 'Extension path is empty or contains null bytes')
  }

  for (let i = 0; i < extensionPath.length; i++) {
    if (extensionPath.charCodeAt(i) <= 0x1f) {
      throw new ExtensionError(extensionPath, 'Extension path contains control characters')
    }
  }

  const segments = extensionPath.split(/[/\\]/)
  if (segments.includes('..')) {
    throw new ExtensionError(extensionPath, 'Extension path must not contain directory traversal segments')
  }

  const { resolve } = await import('node:path')
  const resolved = resolve(extensionPath)

  try {
    const escaped = resolved.replace(/'/g, "''")
    await writer.exec(`SELECT load_extension('${escaped}')`)
  } catch (err) {
    throw new ExtensionError(extensionPath, err instanceof Error ? err.message : String(err))
  }
}
