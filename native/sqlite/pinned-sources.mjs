import { execFileSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const sourceDirectory = dirname(fileURLToPath(import.meta.url))
const sourcesCache = join(sourceDirectory, 'out', 'sources')

export const SQLITE3MC_VERSION = '2.6.1'
const SQLITE3MC_SQLITE_VERSION = '3.54.0'

const SQLITE3MC_SOURCE = {
  name: `sqlite3mc-${SQLITE3MC_VERSION}-sqlite-${SQLITE3MC_SQLITE_VERSION}-amalgamation.zip`,
  url: `https://github.com/utelle/SQLite3MultipleCiphers/releases/download/v${SQLITE3MC_VERSION}/sqlite3mc-${SQLITE3MC_VERSION}-sqlite-${SQLITE3MC_SQLITE_VERSION}-amalgamation.zip`,
  sha256: '3f25af603566a23c821d8e581778b5ece2a541d2435776de57b8d845c176815a',
}

const EXPO_SQLITE_TARBALL_SHA256 = {
  '15.0.0': '942ae5f760fb5faca3a76b37bbef17738c4459031345450149afba9825e04073',
  '58.0.15': '8fdd510a23d1114b7654c5b5c1f06758c96a6d33d5fa61e8c52e6eb4e9246deb',
}

export const EXPO_SQLITE_VERSIONS = Object.keys(EXPO_SQLITE_TARBALL_SHA256)

function expoSqliteSource(version) {
  const sha256 = EXPO_SQLITE_TARBALL_SHA256[version]
  if (sha256 === undefined) {
    throw new Error(`The build pins expo-sqlite ${EXPO_SQLITE_VERSIONS.join(' and ')}, not ${version}`)
  }
  return {
    name: `expo-sqlite-${version}.tgz`,
    url: `https://registry.npmjs.org/expo-sqlite/-/expo-sqlite-${version}.tgz`,
    sha256,
  }
}

const OPENSSL_HEADERS_NODE_VERSION = '22.23.4'

const NODE_HEADERS_SOURCE = {
  name: `node-v${OPENSSL_HEADERS_NODE_VERSION}-headers.tar.gz`,
  url: `https://nodejs.org/dist/v${OPENSSL_HEADERS_NODE_VERSION}/node-v${OPENSSL_HEADERS_NODE_VERSION}-headers.tar.gz`,
  sha256: 'dc4028dc105e27e5295119dd9da076fca71d5216113e097813f0fb30f59241a5',
  topDirectory: `node-v${OPENSSL_HEADERS_NODE_VERSION}`,
}

const CACHE_COMPLETE_MARKER = '.complete'

function sha256Of(bytes) {
  return createHash('sha256').update(bytes).digest('hex')
}

async function fetchPinned(source) {
  mkdirSync(sourcesCache, { recursive: true })
  const cached = join(sourcesCache, source.name)
  if (existsSync(cached) && sha256Of(readFileSync(cached)) === source.sha256) return cached
  const response = await fetch(source.url, { redirect: 'follow' })
  if (!response.ok) {
    throw new Error(`Downloading ${source.url} failed with HTTP ${response.status}`)
  }
  const bytes = Buffer.from(await response.arrayBuffer())
  const digest = sha256Of(bytes)
  if (digest !== source.sha256) {
    throw new Error(
      `${source.url} has SHA-256 ${digest}, and the build pins ${source.sha256}, so the build refuses to use it`,
    )
  }
  const staged = `${cached}.${process.pid}.partial`
  writeFileSync(staged, bytes)
  renameSync(staged, cached)
  return cached
}

const EXPO_SYMBOL_PREFIX = /\bexsqlite3/g

function restoreSqliteSymbols(path) {
  writeFileSync(path, readFileSync(path, 'utf8').replace(EXPO_SYMBOL_PREFIX, 'sqlite3'))
}

async function amalgamationFrom(source, directoryName, unpack, files, transform = () => {}) {
  const amalgamationDirectory = join(sourcesCache, directoryName)
  if (existsSync(join(amalgamationDirectory, CACHE_COMPLETE_MARKER))) return amalgamationDirectory
  const archive = await fetchPinned(source)
  const workDirectory = mkdtempSync(join(tmpdir(), 'sirannon-amalgamation-'))
  try {
    unpack(archive, workDirectory)
    rmSync(amalgamationDirectory, { recursive: true, force: true })
    mkdirSync(amalgamationDirectory, { recursive: true })
    for (const file of files) {
      copyFileSync(join(workDirectory, file.from), join(amalgamationDirectory, file.to))
      transform(join(amalgamationDirectory, file.to))
    }
    writeFileSync(join(amalgamationDirectory, CACHE_COMPLETE_MARKER), source.sha256)
    return amalgamationDirectory
  } finally {
    rmSync(workDirectory, { recursive: true, force: true })
  }
}

export function sqlite3mcAmalgamationDirectory() {
  const files = [
    { from: 'sqlite3mc_amalgamation.c', to: 'sqlite3.c' },
    { from: 'sqlite3mc_amalgamation.h', to: 'sqlite3.h' },
  ]
  return amalgamationFrom(
    SQLITE3MC_SOURCE,
    `sqlite3mc-${SQLITE3MC_VERSION}-amalgamation`,
    (archive, workDirectory) => {
      execFileSync('unzip', ['-q', archive, ...files.map(file => file.from), '-d', workDirectory], {
        stdio: 'inherit',
      })
    },
    files,
  )
}

export function expoSqlcipherAmalgamationDirectory(version) {
  const files = [
    { from: 'package/vendor/sqlcipher/sqlite3.c', to: 'sqlite3.c' },
    { from: 'package/vendor/sqlcipher/sqlite3.h', to: 'sqlite3.h' },
  ]
  return amalgamationFrom(
    expoSqliteSource(version),
    `expo-sqlite-${version}-sqlcipher`,
    (archive, workDirectory) => {
      execFileSync('tar', ['-xzf', archive, '-C', workDirectory, ...files.map(file => file.from)], {
        stdio: 'inherit',
      })
    },
    files,
    restoreSqliteSymbols,
  )
}

export async function nodeOpenSslIncludeDirectory() {
  const headersDirectory = join(sourcesCache, `node-v${OPENSSL_HEADERS_NODE_VERSION}-openssl-headers`)
  const includeDirectory = join(headersDirectory, NODE_HEADERS_SOURCE.topDirectory, 'include', 'node')
  if (existsSync(join(headersDirectory, CACHE_COMPLETE_MARKER))) return includeDirectory
  const archive = await fetchPinned(NODE_HEADERS_SOURCE)
  rmSync(headersDirectory, { recursive: true, force: true })
  mkdirSync(headersDirectory, { recursive: true })
  execFileSync(
    'tar',
    ['-xzf', archive, '-C', headersDirectory, `${NODE_HEADERS_SOURCE.topDirectory}/include/node/openssl`],
    { stdio: 'inherit' },
  )
  writeFileSync(join(headersDirectory, CACHE_COMPLETE_MARKER), NODE_HEADERS_SOURCE.sha256)
  return includeDirectory
}
