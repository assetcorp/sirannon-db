import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { copyBetterSqliteJavaScript } from './better-sqlite3-source.mjs'
import { betterSqliteDefines, buildAddon, hostTarget, resolveTarget } from './compile.mjs'
import { sqlite3mcAmalgamationDirectory } from './pinned-sources.mjs'

const sourceDirectory = dirname(fileURLToPath(import.meta.url))

const SQLITE3MC_DEFINES = [
  'SQLITE_CORE=1',
  'SQLITE_TEMP_STORE=2',
  'SQLITE_ENABLE_SESSION',
  'SQLITE_ENABLE_PREUPDATE_HOOK',
  'CODEC_TYPE=CODEC_TYPE_AEGIS',
  'HAVE_CIPHER_AEGIS=1',
  'HAVE_CIPHER_AES_128_CBC=0',
  'HAVE_CIPHER_AES_256_CBC=0',
  'HAVE_CIPHER_CHACHA20=0',
  'HAVE_CIPHER_SQLCIPHER=0',
  'HAVE_CIPHER_RC4=0',
  'HAVE_CIPHER_ASCON128=0',
]

const SIRANNON_SQLITE = {
  description: "Sirannon's SQLite",
  amalgamationDirectory: sqlite3mcAmalgamationDirectory,
  includeDirectories: async () => [],
  sqliteDefines: betterSqliteDirectory => [...betterSqliteDefines(betterSqliteDirectory), ...SQLITE3MC_DEFINES],
  addonDefines: ['SQLITE_HAS_CODEC', 'SQLITE_ENABLE_SESSION', 'SQLITE_ENABLE_PREUPDATE_HOOK'],
  sqlitePatchDirectory: join(sourceDirectory, 'patches', 'sqlite'),
  betterSqlitePatchDirectory: join(sourceDirectory, 'patches', 'better-sqlite3'),
  appleFrameworks: [],
  outputDirectoryName: name => name,
  supports: () => true,
}

const requested = process.argv[2] ?? hostTarget()
try {
  const target = resolveTarget(SIRANNON_SQLITE, requested)
  console.log(`Built ${await buildAddon(SIRANNON_SQLITE, requested, target)}`)
  console.log(`Copied better-sqlite3's JavaScript to ${copyBetterSqliteJavaScript()}`)
} catch (err) {
  console.error(`Building Sirannon's SQLite for ${requested} failed: ${err.message}`)
  process.exit(1)
}
