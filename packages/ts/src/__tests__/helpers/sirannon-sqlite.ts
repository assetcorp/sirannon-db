import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { usesMuslLibc } from '../../drivers/vfs-library.js'

const repositoryRoot = join(dirname(fileURLToPath(import.meta.url)), '..', '..', '..', '..', '..')

export const SIRANNON_SQLITE_REQUIRED = process.env.SIRANNON_REQUIRE_SIRANNON_SQLITE === '1'

export const EXPO_SQLCIPHER_REQUIRED = process.env.SIRANNON_REQUIRE_EXPO_SQLCIPHER === '1'

function nativeTarget(): string {
  return `${process.platform}-${process.arch}${process.platform === 'linux' && usesMuslLibc() ? '-musl' : ''}`
}

export function builtSirannonSqliteBindingPath(): string | null {
  const binding = join(repositoryRoot, 'native', 'sqlite', 'out', nativeTarget(), 'better_sqlite3.node')
  const javascript = join(repositoryRoot, 'native', 'sqlite', 'npm', 'sirannon-sqlite', 'lib', 'database.js')
  return existsSync(binding) && existsSync(javascript) ? binding : null
}

export const EXPO_SQLITE_SQLCIPHER_VERSIONS = ['15.0.0', '58.0.15'] as const

export function builtExpoSqlcipherBindingPath(version: string): string | null {
  const binding = join(
    repositoryRoot,
    'native',
    'sqlite',
    'out',
    `expo-sqlcipher-${version}-${nativeTarget()}`,
    'better_sqlite3.node',
  )
  return existsSync(binding) ? binding : null
}

export function builtExpoSqlcipherBindings(): { version: string; binding: string | null }[] {
  return EXPO_SQLITE_SQLCIPHER_VERSIONS.map(version => ({ version, binding: builtExpoSqlcipherBindingPath(version) }))
}

export function stockBetterSqliteBindingPath(): string {
  const target = `${process.platform === 'linux' && usesMuslLibc() ? 'linuxmusl' : process.platform}-${process.arch}`
  return join(repositoryRoot, 'packages', 'ts', 'node_modules', 'better-sqlite3', 'prebuilds', `${target}.node`)
}
