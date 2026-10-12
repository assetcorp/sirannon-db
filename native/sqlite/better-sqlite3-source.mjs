import { cpSync, readFileSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const sourceDirectory = dirname(fileURLToPath(import.meta.url))
const referenceManifest = join(sourceDirectory, '..', '..', 'packages', 'ts', 'package.json')

export const PATCHED_BETTER_SQLITE3_VERSION = '13.0.2'
export const ROW_COUNTING_PACKAGE_DIRECTORY = join(sourceDirectory, 'npm', 'sirannon-sqlite')

export function referenceRequire() {
  return createRequire(pathToFileURL(referenceManifest))
}

export function resolveBetterSqliteDirectory() {
  const betterSqliteDirectory = dirname(referenceRequire().resolve('better-sqlite3/package.json'))
  const { version } = JSON.parse(readFileSync(join(betterSqliteDirectory, 'package.json'), 'utf8'))
  if (version !== PATCHED_BETTER_SQLITE3_VERSION) {
    throw new Error(
      `The row-count patches apply to better-sqlite3 ${PATCHED_BETTER_SQLITE3_VERSION}, and packages/ts resolves better-sqlite3 ${version}`,
    )
  }
  return betterSqliteDirectory
}

export function copyBetterSqliteJavaScript() {
  const libraryDirectory = join(ROW_COUNTING_PACKAGE_DIRECTORY, 'lib')
  rmSync(libraryDirectory, { recursive: true, force: true })
  cpSync(join(resolveBetterSqliteDirectory(), 'lib'), libraryDirectory, { recursive: true })
  return libraryDirectory
}
