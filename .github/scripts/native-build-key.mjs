import { execFileSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { appendFileSync, readdirSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join, relative, sep } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { hostTarget, TARGETS } from '../../native/sqlite/compile.mjs'
import { EXPO_SQLITE_VERSIONS } from '../../native/sqlite/pinned-sources.mjs'

const KEY_FORMAT = 1
const REPOSITORY_ROOT = join(dirname(fileURLToPath(import.meta.url)), '..', '..')
const REFERENCE_MANIFEST = join(REPOSITORY_ROOT, 'packages', 'ts', 'package.json')
const GIT_OUTPUT_LIMIT_BYTES = 64 * 1024 * 1024
const SQLITE_BUILD_SOURCES = { tracked: ['native/sqlite/'], trackedExcept: ['native/sqlite/npm/'] }
const SQLITE_PACKAGES = ['better-sqlite3', 'node-api-headers', 'node-addon-api']

const BINARIES = {
  vfs: {
    script: 'build:vfs',
    tracked: ['native/vfs/'],
    trackedExcept: [],
    packages: ['better-sqlite3'],
    supports: () => true,
    paths: target => {
      const libc = target.endsWith('-musl') ? '/musl' : ''
      const platform = target.replace(/-musl$/, '')
      const extension = platform.startsWith('win32') ? 'dll' : platform.startsWith('darwin') ? 'dylib' : 'so'
      return [`native/npm/${platform}${libc}/sirannonvfs.${extension}`]
    },
  },
  sqlite: {
    script: 'build:sqlite',
    ...SQLITE_BUILD_SOURCES,
    packages: SQLITE_PACKAGES,
    supports: () => true,
    paths: target => [`native/sqlite/out/${target}/better_sqlite3.node`],
  },
  'expo-sqlcipher': {
    script: 'build:expo-sqlcipher',
    ...SQLITE_BUILD_SOURCES,
    packages: SQLITE_PACKAGES,
    supports: target => TARGETS[target].importMachine === undefined,
    paths: target =>
      EXPO_SQLITE_VERSIONS.map(version => `native/sqlite/out/expo-sqlcipher-${version}-${target}/better_sqlite3.node`),
  },
}

function git(args) {
  return execFileSync('git', args, { cwd: REPOSITORY_ROOT, encoding: 'utf8', maxBuffer: GIT_OUTPUT_LIMIT_BYTES })
}

function trackedFiles(binary) {
  const listed = git(['ls-files', '-z', '--', ...binary.tracked])
    .split('\0')
    .filter(path => path.length > 0)
  const files = listed.filter(path => !binary.trackedExcept.some(prefix => path.startsWith(prefix))).sort()
  if (files.length === 0) throw new Error(`git tracks no file under ${binary.tracked.join(', ')}`)
  return files
}

function filesBelow(directory) {
  const files = []
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.name === 'node_modules') continue
    const path = join(directory, entry.name)
    if (entry.isDirectory()) files.push(...filesBelow(path))
    else if (entry.isFile()) files.push(path)
    else throw new Error(`${path} is neither a file nor a directory, so the key cannot cover it`)
  }
  return files
}

function packageDirectory(name) {
  const fromReference = createRequire(pathToFileURL(REFERENCE_MANIFEST))
  if (name !== 'node-addon-api') return dirname(fromReference.resolve(`${name}/package.json`))
  const betterSqlite = dirname(fromReference.resolve('better-sqlite3/package.json'))
  return dirname(createRequire(join(betterSqlite, 'package.json')).resolve('node-addon-api'))
}

function compilerIdentity(target) {
  if (TARGETS[target].appleArchitecture) {
    return [
      execFileSync('cc', ['--version'], { encoding: 'utf8' }),
      execFileSync('xcrun', ['--sdk', 'macosx', '--show-sdk-version'], { encoding: 'utf8' }),
    ].join('\n')
  }
  return `zig ${execFileSync('zig', ['version'], { encoding: 'utf8' }).trim()}`
}

function buildScript(binary) {
  const script = JSON.parse(readFileSync(REFERENCE_MANIFEST, 'utf8')).scripts?.[binary.script]
  if (typeof script !== 'string') throw new Error(`packages/ts/package.json defines no ${binary.script} script`)
  return script
}

function inputDigest(name, binary, target) {
  const digest = createHash('sha256')
  const record = (label, bytes) => {
    digest.update(`${label}\0${bytes.length}\0`)
    digest.update(bytes)
  }
  record('format', Buffer.from(String(KEY_FORMAT)))
  record('binary', Buffer.from(name))
  record('target', Buffer.from(target))
  record('script', Buffer.from(buildScript(binary)))
  record('compiler', Buffer.from(compilerIdentity(target)))
  for (const path of trackedFiles(binary)) record(`tracked ${path}`, readFileSync(join(REPOSITORY_ROOT, path)))
  for (const packageName of binary.packages) {
    const directory = packageDirectory(packageName)
    for (const file of filesBelow(directory).sort()) {
      record(`package ${packageName} ${relative(directory, file).split(sep).join('/')}`, readFileSync(file))
    }
  }
  return digest.digest('hex')
}

function writeOutputs(outputs) {
  const target = process.env.GITHUB_OUTPUT
  for (const [key, value] of Object.entries(outputs)) {
    console.log(`${key}=${value}`)
    if (target === undefined || target === '') continue
    const delimiter = `NATIVE_BUILD_KEY_${createHash('sha256').update(value).digest('hex')}`
    appendFileSync(target, `${key}<<${delimiter}\n${value}\n${delimiter}\n`)
  }
}

function main() {
  const [name, requestedTarget] = process.argv.slice(2)
  const binary = BINARIES[name]
  if (binary === undefined) {
    throw new Error(`No native binary is called ${JSON.stringify(name)}. Known: ${Object.keys(BINARIES).join(', ')}`)
  }
  const target = requestedTarget === undefined || requestedTarget === '' ? hostTarget() : requestedTarget
  if (!Object.hasOwn(TARGETS, target) || !binary.supports(target)) {
    throw new Error(`The ${name} binary has no build target called ${JSON.stringify(target)}`)
  }
  writeOutputs({
    key: `sirannon-native-${name}-${target}-${inputDigest(name, binary, target)}`,
    target,
    path: binary.paths(target).join('\n'),
  })
}

try {
  main()
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error))
  process.exit(1)
}
