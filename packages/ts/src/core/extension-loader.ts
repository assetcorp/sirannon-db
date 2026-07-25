import type { SQLiteConnection, SQLiteDriver } from './driver/types.js'
import { ExtensionError } from './errors.js'

export async function loadExtension(
  driver: SQLiteDriver,
  writer: SQLiteConnection,
  extensionPath: string,
): Promise<void> {
  if (!driver.capabilities.extensions || !driver.resolveExtensionPath) {
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

  const resolved = driver.resolveExtensionPath(extensionPath)

  try {
    const escaped = resolved.replace(/'/g, "''")
    await writer.exec(`SELECT load_extension('${escaped}')`)
  } catch (err) {
    throw new ExtensionError(extensionPath, err instanceof Error ? err.message : String(err))
  }
}
