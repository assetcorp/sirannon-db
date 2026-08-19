import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
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
