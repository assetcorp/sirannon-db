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
 * Loads a compiled SQLite extension into every connection given, so both reads
 * and writes can call the extension's functions. Each runtime loads through its
 * own call rather than the SQL `load_extension` function, which both Node
 * drivers refuse as unauthorised.
 *
 * A runtime that cannot load an extension refuses through its own connection,
 * so the error names that runtime.
 *
 * SQLite has no call that unloads an extension, so where one connection in the
 * set fails, the connections loaded before it keep the extension and this
 * function reports the failure.
 *
 * @param driver - Driver that reports extension support and resolves the path.
 * @param connections - Every connection that must be able to call the extension's functions.
 * @param extensionPath - Path to the compiled extension.
 * @returns The absolute path the driver resolved, which a connection opened later loads.
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
