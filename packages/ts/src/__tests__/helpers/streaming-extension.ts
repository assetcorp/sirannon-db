import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { SQLiteDriver } from '../../core/driver/types.js'
import { usesMuslLibc, vfsLibrarySegments } from '../../drivers/vfs-library.js'

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

const FIRST_NODE_MAJOR_THAT_OPENS_A_COPY_BY_URI = 23

export function nodeSqliteParsesBackupUris(): boolean {
  return Number.parseInt(process.versions.node, 10) >= FIRST_NODE_MAJOR_THAT_OPENS_A_COPY_BY_URI
}

export function driverStreamsToDestination(driver: SQLiteDriver): boolean {
  return driver.createBackupEngine?.(driver).streamsToDestination() === true
}
