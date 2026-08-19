import { existsSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'

const LIBRARY_FILE_NAMES: Record<string, string> = {
  darwin: 'sirannonvfs.dylib',
  win32: 'sirannonvfs.dll',
}

const ELF_LIBRARY_FILE_NAME = 'sirannonvfs.so'
const MUSL_DIRECTORY = 'musl'

const PLATFORMS_WITH_A_BINARY = new Set([
  'darwin-arm64',
  'darwin-x64',
  'linux-arm64',
  'linux-x64',
  'win32-arm64',
  'win32-x64',
])

let muslLibc: boolean | undefined

/**
 * Reports whether this host links against musl rather than glibc. Alpine does,
 * and most other distributions do not. A shared library built for one fails to
 * load on the other, so Linux carries a binary for each.
 *
 * @returns Whether the C library here is musl.
 *
 * @internal
 */
export function usesMuslLibc(): boolean {
  if (muslLibc === undefined) {
    try {
      const report = process.report.getReport() as { header?: { glibcVersionRuntime?: string } }
      muslLibc = report.header?.glibcVersionRuntime === undefined
    } catch {
      muslLibc = false
    }
  }
  return muslLibc
}

/**
 * Names the file the compiled extension is published under. Each platform
 * loads shared libraries under its own file extension.
 *
 * @param platform - Platform name, in the form Node reports it.
 * @returns The file name to look for.
 *
 * @internal
 */
export function vfsLibraryFileName(platform: string): string {
  return LIBRARY_FILE_NAMES[platform] ?? ELF_LIBRARY_FILE_NAME
}

/**
 * Gives the path of the compiled extension inside its package, relative to the
 * package root. SQLite reads the entry point's name from the file name, so the
 * musl build keeps the same file name in a directory of its own.
 *
 * @param platform - Platform name, in the form Node reports it.
 * @param muslLibc - Whether this host links against musl.
 * @returns The path segments that lead to the library.
 *
 * @internal
 */
export function vfsLibrarySegments(platform: string, muslLibc: boolean): string[] {
  const fileName = vfsLibraryFileName(platform)
  return platform === 'linux' && muslLibc ? [MUSL_DIRECTORY, fileName] : [fileName]
}

/**
 * Names the package that carries the compiled extension for one platform. Each
 * platform has a package of its own, so an install fetches the one binary this
 * host runs.
 *
 * @param platform - Platform name, in the form Node reports it.
 * @param architecture - Processor architecture, in the form Node reports it.
 * @returns The package name, or null where that pair has no published binary.
 *
 * @internal
 */
export function vfsPackageName(platform: string, architecture: string): string | null {
  const target = `${platform}-${architecture}`
  return PLATFORMS_WITH_A_BINARY.has(target) ? `@delali/sirannon-vfs-${target}` : null
}

/**
 * Finds the compiled extension the install fetched for this host.
 *
 * @param platform - Platform name, in the form Node reports it.
 * @param architecture - Processor architecture, in the form Node reports it.
 * @returns The absolute path of the library, or null where this host has none.
 *
 * @internal
 */
export function resolveVfsExtensionPath(
  platform: string = process.platform,
  architecture: string = process.arch,
): string | null {
  const packageName = vfsPackageName(platform, architecture)
  if (!packageName) return null
  try {
    const manifest = createRequire(import.meta.url).resolve(`${packageName}/package.json`)
    const library = join(dirname(manifest), ...vfsLibrarySegments(platform, usesMuslLibc()))
    return existsSync(library) ? library : null
  } catch {
    return null
  }
}
