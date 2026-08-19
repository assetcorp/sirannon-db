import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { SQLiteDriver } from '../../core/driver/types.js'
import { usesMuslLibc, vfsLibrarySegments } from '../../drivers/vfs-library.js'

/**
 * Finds the streaming extension this repository built for the host it runs on.
 * Returns null where nobody has run `pnpm build:vfs`, so a caller can skip the
 * test rather than fail it.
 */
export function builtStreamingExtensionPath(): string | null {
  const packageRoot = join(dirname(fileURLToPath(import.meta.url)), '..', '..', '..')
  const library = join(
    packageRoot,
    '..',
    '..',
    'native',
    'npm',
    `${process.platform}-${process.arch}`,
    ...vfsLibrarySegments(process.platform, usesMuslLibc()),
  )
  return existsSync(library) ? library : null
}

/**
 * Reports whether a driver streams a copy on this host. The compiled library is
 * one of the conditions and the runtime supplies the rest. A test that gates on
 * the library alone therefore fails on a host that takes the staged route,
 * rather than skipping.
 */
export function driverStreamsToDestination(driver: SQLiteDriver): boolean {
  return driver.createBackupEngine?.(driver).streamsToDestination() === true
}
