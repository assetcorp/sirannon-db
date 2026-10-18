import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { usesMuslLibc } from '../../drivers/vfs-library.js'

const repositoryRoot = join(dirname(fileURLToPath(import.meta.url)), '..', '..', '..', '..', '..')

export const SIRANNON_SQLITE_REQUIRED = process.env.SIRANNON_REQUIRE_SIRANNON_SQLITE === '1'

export function builtSirannonSqliteBindingPath(): string | null {
  const target = `${process.platform}-${process.arch}${process.platform === 'linux' && usesMuslLibc() ? '-musl' : ''}`
  const binding = join(repositoryRoot, 'native', 'sqlite', 'out', target, 'better_sqlite3.node')
  const javascript = join(repositoryRoot, 'native', 'sqlite', 'npm', 'sirannon-sqlite', 'lib', 'database.js')
  return existsSync(binding) && existsSync(javascript) ? binding : null
}

export function stockBetterSqliteBindingPath(): string {
  const target = `${process.platform === 'linux' && usesMuslLibc() ? 'linuxmusl' : process.platform}-${process.arch}`
  return join(repositoryRoot, 'packages', 'ts', 'node_modules', 'better-sqlite3', 'prebuilds', `${target}.node`)
}
