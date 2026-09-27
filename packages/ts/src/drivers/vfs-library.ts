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
 * Returns `true` when the Node process report names no glibc version, which on
 * Linux means that the C library is musl, as on Alpine. The dynamic loader
 * rejects a shared library built for the other C library, so each Linux package
 * ships one build for glibc and one for musl.
 *
 * @returns `true` when the process report names no glibc version.
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
 * Returns the file name of the compiled extension for a platform, which ends in
 * `.dylib` on macOS, `.dll` on Windows, and `.so` elsewhere.
 *
 * @param platform - The platform name, in the form of `process.platform`.
 * @returns The library file name.
 *
 * @internal
 */
export function vfsLibraryFileName(platform: string): string {
  return LIBRARY_FILE_NAMES[platform] ?? ELF_LIBRARY_FILE_NAME
}

/**
 * Returns the path of the compiled extension inside its package, relative to
 * the package root. SQLite derives the entry point's name from the file name,
 * so the package stores the musl build under the same file name in a `musl`
 * directory.
 *
 * @param platform - The platform name, in the form of `process.platform`.
 * @param muslLibc - `true` when the host uses musl.
 * @returns The path segments of the library, relative to the package root.
 *
 * @internal
 */
export function vfsLibrarySegments(platform: string, muslLibc: boolean): string[] {
  const fileName = vfsLibraryFileName(platform)
  return platform === 'linux' && muslLibc ? [MUSL_DIRECTORY, fileName] : [fileName]
}

/**
 * Returns the name of the package that ships the compiled extension for one
 * platform and architecture. Each pair has its own package, which declares its
 * `os` and `cpu` fields.
 *
 * @param platform - The platform name, in the form of `process.platform`.
 * @param architecture - The processor architecture, in the form of `process.arch`.
 * @returns The package name, or `null` for a pair outside the published set.
 *
 * @internal
 */
export function vfsPackageName(platform: string, architecture: string): string | null {
  const target = `${platform}-${architecture}`
  return PLATFORMS_WITH_A_BINARY.has(target) ? `@delali/sirannon-vfs-${target}` : null
}

/**
 * Returns the absolute path of the compiled extension in the platform package that is installed for this host.
 *
 * @param platform - The platform name, in the form of `process.platform`.
 * @param architecture - The processor architecture, in the form of `process.arch`.
 * @returns The absolute path of the library, or `null` when the package or the library file is missing.
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
