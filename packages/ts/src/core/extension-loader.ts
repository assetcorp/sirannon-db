import type { SQLiteConnection, SQLiteDriver } from './driver/types.js'
import { ExtensionError, SirannonError } from './errors.js'

function assertPathIsSafe(extensionPath: string): void {
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
}

function isAbsolutePath(candidate: string): boolean {
  return candidate.startsWith('/') || candidate.startsWith('\\') || /^[A-Za-z]:[/\\]/.test(candidate)
}

/**
 * Loads a compiled SQLite extension into every connection that the caller
 * passes, so that reads and writes can both call the extension's functions.
 * Each driver loads the extension through its runtime's own call, because both
 * Node drivers reject the SQL `load_extension` function as unauthorised.
 *
 * A driver whose runtime has no loading call throws its own error from the
 * connection, and that error names the runtime.
 *
 * SQLite has no call that unloads an extension, so when one connection in the
 * set fails, the connections loaded before it keep the extension and this
 * function throws the failure.
 *
 * @param driver - The driver, which declares extension support and resolves the path.
 * @param connections - Every connection that must be able to call the extension's functions.
 * @param extensionPath - The path to the compiled extension.
 * @returns The absolute path that the driver resolved, which Sirannon loads into each connection that it opens later.
 */
export async function loadExtension(
  driver: SQLiteDriver,
  connections: readonly SQLiteConnection[],
  extensionPath: string,
): Promise<string> {
  assertPathIsSafe(extensionPath)

  if (connections.some(connection => connection.loadExtension === undefined)) {
    throw new ExtensionError(
      extensionPath,
      driver.capabilities.extensions
        ? 'The current driver declares extension support but opens connections with no loading call'
        : 'Extensions are not supported by the current driver',
    )
  }

  if (driver.capabilities.extensions && !driver.resolveExtensionPath) {
    throw new ExtensionError(
      extensionPath,
      'The current driver declares extension support but resolves no absolute path, which would let the dynamic linker search its own paths',
    )
  }

  const resolved = driver.resolveExtensionPath?.(extensionPath) ?? extensionPath

  if (driver.resolveExtensionPath && !isAbsolutePath(resolved)) {
    throw new ExtensionError(
      extensionPath,
      'The current driver resolved the extension to a relative path, which would let the dynamic linker search its own paths',
    )
  }

  for (const connection of connections) {
    try {
      await connection.loadExtension?.(resolved)
    } catch (err) {
      if (err instanceof SirannonError) throw err
      throw new ExtensionError(extensionPath, err instanceof Error ? err.message : String(err))
    }
  }

  return resolved
}
