import { ExtensionError, SirannonError } from '../errors.js'

/**
 * Calls the runtime's own extension loading function and rethrows any failure
 * as an {@link ExtensionError}, so that every driver throws the same error type
 * when an extension fails to load. A `SirannonError` from the runtime
 * propagates unchanged.
 *
 * @param extensionPath - The absolute path that the error message includes.
 * @param load - The runtime's loading function.
 * @returns A promise that resolves once the runtime loads the extension.
 */
export async function loadThroughRuntime(extensionPath: string, load: () => void): Promise<void> {
  try {
    load()
  } catch (err) {
    if (err instanceof SirannonError) throw err
    throw new ExtensionError(extensionPath, err instanceof Error ? err.message : String(err))
  }
}
