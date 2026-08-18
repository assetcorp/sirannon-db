import { ExtensionError } from '../errors.js'

/**
 * Runs a runtime's own extension loading call and reports any failure as an
 * {@link ExtensionError}, so every driver raises one error type for one cause.
 *
 * @param extensionPath - Absolute path the error names.
 * @param load - The runtime's loading call.
 */
export async function loadThroughRuntime(extensionPath: string, load: () => void): Promise<void> {
  try {
    load()
  } catch (err) {
    throw new ExtensionError(extensionPath, err instanceof Error ? err.message : String(err))
  }
}
