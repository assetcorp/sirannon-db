import { buildAddon, hostTarget, resolveTarget } from './compile.mjs'
import {
  EXPO_SQLITE_VERSIONS,
  expoSqlcipherAmalgamationDirectory,
  nodeOpenSslIncludeDirectory,
} from './pinned-sources.mjs'

const EXPO_SQLITE_ANDROID_DEFINES = {
  '15.0.0': [
    'SQLITE_ENABLE_BYTECODE_VTAB=1',
    'SQLITE_TEMP_STORE=2',
    'SQLITE_ENABLE_FTS4=1',
    'SQLITE_ENABLE_FTS3_PARENTHESIS=1',
    'SQLITE_ENABLE_FTS5=1',
    'SQLITE_HAS_CODEC=1',
  ],
  '58.0.15': [
    'SQLITE_ENABLE_BYTECODE_VTAB=1',
    'SQLITE_TEMP_STORE=2',
    'SQLITE_ENABLE_SESSION=1',
    'SQLITE_ENABLE_PREUPDATE_HOOK=1',
    'SQLITE_ENABLE_MATH_FUNCTIONS=1',
    'SQLITE_ENABLE_API_ARMOR=1',
    'SQLITE_ENABLE_FTS4=1',
    'SQLITE_ENABLE_FTS3_PARENTHESIS=1',
    'SQLITE_ENABLE_FTS5=1',
    'SQLITE_HAS_CODEC=1',
    'SQLITE_EXTRA_INIT=sqlcipher_extra_init',
    'SQLITE_EXTRA_SHUTDOWN=sqlcipher_extra_shutdown',
  ],
}
const BETTER_SQLITE3_BINDING_DEFINES = ['SQLITE_ENABLE_COLUMN_METADATA']
const OPENSSL_CRYPTO_DEFINE = 'SQLCIPHER_CRYPTO_OPENSSL'
const COMMONCRYPTO_CRYPTO_DEFINE = 'SQLCIPHER_CRYPTO_CC'
const APPLE_CRYPTO_FRAMEWORKS = ['-framework', 'Security', '-framework', 'CoreFoundation']

function expoSqlcipherEngine(version) {
  const androidDefines = EXPO_SQLITE_ANDROID_DEFINES[version]
  if (androidDefines === undefined) throw new Error(`No build flags are recorded for expo-sqlite ${version}`)
  return {
    description: `The SQLCipher of expo-sqlite ${version}, which only the tests load`,
    amalgamationDirectory: () => expoSqlcipherAmalgamationDirectory(version),
    includeDirectories: async target => (target.appleArchitecture ? [] : [await nodeOpenSslIncludeDirectory()]),
    sqliteDefines: (_betterSqliteDirectory, target) => [
      ...androidDefines,
      ...BETTER_SQLITE3_BINDING_DEFINES,
      target.appleArchitecture ? COMMONCRYPTO_CRYPTO_DEFINE : OPENSSL_CRYPTO_DEFINE,
    ],
    addonDefines: [],
    sqlitePatchDirectory: null,
    betterSqlitePatchDirectory: null,
    appleFrameworks: APPLE_CRYPTO_FRAMEWORKS,
    outputDirectoryName: name => `expo-sqlcipher-${version}-${name}`,
    supports: target => target.importMachine === undefined,
  }
}

const requested = process.argv[2] ?? hostTarget()
for (const version of EXPO_SQLITE_VERSIONS) {
  try {
    const engine = expoSqlcipherEngine(version)
    const target = resolveTarget(engine, requested)
    console.log(`Built ${await buildAddon(engine, requested, target)}`)
  } catch (err) {
    console.error(`Building the SQLCipher of expo-sqlite ${version} for ${requested} failed: ${err.message}`)
    process.exit(1)
  }
}
