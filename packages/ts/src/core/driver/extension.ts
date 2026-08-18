import { ExtensionError, SirannonError } from '../errors.js'

/**
 * Runs a runtime's own extension loading call, so every driver reports one
 * error type for one cause. A `SirannonError` from the runtime passes through
 * unchanged, and any other failure becomes an {@link ExtensionError}.
 *
 * @param extensionPath - Absolute path the failure message reports.
 * @param load - The runtime's loading call.
 * @returns A promise that settles once the runtime has loaded the extension.
 */
export async function loadThroughRuntime(extensionPath: string, load: () => void): Promise<void> {
  try {
    load()
  } catch (err) {
    if (err instanceof SirannonError) throw err
    throw new ExtensionError(extensionPath, err instanceof Error ? err.message : String(err))
  }
}
