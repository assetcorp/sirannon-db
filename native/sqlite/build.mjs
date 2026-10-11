import { execFileSync, spawnSync } from 'node:child_process'
import { copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const sourceDirectory = dirname(fileURLToPath(import.meta.url))
const outputRoot = join(sourceDirectory, 'out')
const referenceManifest = join(sourceDirectory, '..', '..', 'packages', 'ts', 'package.json')
const sqlitePatch = join(sourceDirectory, 'row-counts.patch')
const statementPatch = join(sourceDirectory, 'statement-row-counts.patch')
const delayLoadHook = join(sourceDirectory, 'win-delay-load-hook.cc')
const linuxExportsMap = join(sourceDirectory, 'linux-exports.map')

const PATCHED_BETTER_SQLITE3_VERSION = '13.0.2'
const NODE_HOST_BINARY = 'node.exe'
const ADDON_FILE_NAME = 'better_sqlite3.node'
const ZIG_LINK_OUTPUT_LIMIT_BYTES = 64 * 1024 * 1024

const TARGETS = {
  'darwin-arm64': { appleArchitecture: 'arm64', macosMinimum: '11.0' },
  'darwin-x64': { appleArchitecture: 'x86_64', macosMinimum: '10.15' },
  'linux-arm64': { zigTriple: 'aarch64-linux-gnu' },
  'linux-arm64-musl': { zigTriple: 'aarch64-linux-musl' },
  'linux-x64': { zigTriple: 'x86_64-linux-gnu' },
  'linux-x64-musl': { zigTriple: 'x86_64-linux-musl' },
  'win32-arm64': { zigTriple: 'aarch64-windows-gnu', importMachine: 'arm64' },
  'win32-x64': { zigTriple: 'x86_64-windows-gnu', importMachine: 'i386:x86-64' },
}

const ADDON_DEFINES = [
  'NAPI_VERSION=10',
  'NAPI_DISABLE_CPP_EXCEPTIONS',
  'NODE_API_SWALLOW_UNTHROWABLE_EXCEPTIONS',
  'BUILDING_NODE_EXTENSION',
  'NODE_GYP_MODULE_NAME=better_sqlite3',
]

function hostTarget() {
  const pair = `${process.platform}-${process.arch}`
  if (process.platform !== 'linux') return pair
  return process.report.getReport().header.glibcVersionRuntime ? pair : `${pair}-musl`
}

function resolveInputs() {
  const fromSirannon = createRequire(pathToFileURL(referenceManifest))
  const betterSqliteDirectory = dirname(fromSirannon.resolve('better-sqlite3/package.json'))
  const { version } = JSON.parse(readFileSync(join(betterSqliteDirectory, 'package.json'), 'utf8'))
  if (version !== PATCHED_BETTER_SQLITE3_VERSION) {
    throw new Error(
      `The row-count patches apply to better-sqlite3 ${PATCHED_BETTER_SQLITE3_VERSION}, and packages/ts resolves better-sqlite3 ${version}`,
    )
  }
  const fromBetterSqlite = createRequire(join(betterSqliteDirectory, 'package.json'))
  const nodeApiHeaders = fromSirannon('node-api-headers')
  return {
    betterSqliteDirectory,
    nodeAddonApiDirectory: dirname(fromBetterSqlite.resolve('node-addon-api')),
    nodeApiIncludeDirectory: nodeApiHeaders.include_dir,
    nodeApiDefinitions: nodeApiHeaders.def_paths.node_api_def,
  }
}

function sqliteDefines(betterSqliteDirectory) {
  const gypi = readFileSync(join(betterSqliteDirectory, 'deps', 'defines.gypi'), 'utf8')
  const defines = [...gypi.matchAll(/^\s*'([A-Z][A-Z0-9_]*(?:=[^']*)?)',?\s*$/gm)].map(match => match[1])
  if (defines.length === 0) {
    throw new Error(`better-sqlite3's deps/defines.gypi lists no SQLite compile options`)
  }
  return defines
}

function run(command, args, options = {}) {
  execFileSync(command, args, { stdio: 'inherit', ...options })
}

function prepareSources(inputs, workDirectory) {
  const sqliteDirectory = join(workDirectory, 'sqlite3')
  mkdirSync(sqliteDirectory, { recursive: true })
  for (const file of ['sqlite3.c', 'sqlite3.h']) {
    copyFileSync(join(inputs.betterSqliteDirectory, 'deps', 'sqlite3', file), join(sqliteDirectory, file))
  }
  cpSync(join(inputs.betterSqliteDirectory, 'src'), join(workDirectory, 'src'), { recursive: true })
  run('git', ['apply', '--whitespace=nowarn', sqlitePatch], { cwd: sqliteDirectory })
  run('git', ['apply', '--whitespace=nowarn', statementPatch], { cwd: workDirectory })
  return sqliteDirectory
}

function compilers(target) {
  if (target.appleArchitecture) {
    const platform = ['-arch', target.appleArchitecture, `-mmacosx-version-min=${target.macosMinimum}`]
    return { c: { command: 'cc', args: platform }, cxx: { command: 'c++', args: [...platform, '-stdlib=libc++'] } }
  }
  return {
    c: { command: 'zig', args: ['cc', '-target', target.zigTriple] },
    cxx: { command: 'zig', args: ['c++', '-target', target.zigTriple] },
  }
}

function compileObjects(target, inputs, workDirectory, sqliteDirectory) {
  const { c, cxx } = compilers(target)
  const positionIndependent = target.importMachine ? [] : ['-fPIC']
  const sqliteObject = join(workDirectory, 'sqlite3.o')
  run(c.command, [
    ...c.args,
    '-c',
    '-O3',
    '-std=c99',
    '-w',
    ...positionIndependent,
    '-DNDEBUG',
    ...sqliteDefines(inputs.betterSqliteDirectory).map(define => `-D${define}`),
    '-I',
    sqliteDirectory,
    '-o',
    sqliteObject,
    join(sqliteDirectory, 'sqlite3.c'),
  ])
  const addonObject = join(workDirectory, 'better_sqlite3.o')
  run(cxx.command, [
    ...cxx.args,
    '-c',
    '-O3',
    '-std=c++20',
    '-fvisibility=hidden',
    '-fvisibility-inlines-hidden',
    ...positionIndependent,
    '-DNDEBUG',
    ...ADDON_DEFINES.map(define => `-D${define}`),
    '-I',
    sqliteDirectory,
    '-I',
    inputs.nodeAddonApiDirectory,
    '-I',
    inputs.nodeApiIncludeDirectory,
    '-o',
    addonObject,
    join(workDirectory, 'src', 'better_sqlite3.cpp'),
  ])
  const objects = [addonObject, sqliteObject]
  if (target.importMachine) {
    const hookObject = join(workDirectory, 'win-delay-load-hook.o')
    run(cxx.command, [...cxx.args, '-c', '-O2', '-o', hookObject, delayLoadHook])
    objects.push(hookObject)
  }
  return objects
}

function zigLinkerArguments(args) {
  const result = spawnSync('zig', args, {
    env: { ...process.env, ZIG_VERBOSE_LINK: '1' },
    encoding: 'utf8',
    maxBuffer: ZIG_LINK_OUTPUT_LIMIT_BYTES,
  })
  if (result.error) throw result.error
  if (result.status !== 0) {
    throw new Error(`zig exited with status ${result.status} while linking: ${result.stderr}`)
  }
  const line = result.stderr.split(/\r?\n/).find(text => text.startsWith('lld-link '))
  if (!line) {
    throw new Error('zig printed no lld-link command, so the build has no link command to add the delay-load flag to')
  }
  const linkerArguments = line.slice('lld-link '.length).trim().split(' ')
  const unknown = linkerArguments.filter(arg => !arg.startsWith('-') && !existsSync(arg))
  if (unknown.length > 0) {
    throw new Error(
      `zig's link command contains arguments that are neither flags nor existing files (${unknown.join(', ')}), which happens when a path contains a space, so move the checkout and zig to paths without spaces`,
    )
  }
  return linkerArguments
}

function linkWindows(target, inputs, workDirectory, objects, addonPath) {
  const importLibrary = join(workDirectory, 'node.lib')
  run('zig', [
    'dlltool',
    '-d',
    inputs.nodeApiDefinitions,
    '-D',
    NODE_HOST_BINARY,
    '-m',
    target.importMachine,
    '-l',
    importLibrary,
  ])
  const { cxx } = compilers(target)
  const linkerArguments = zigLinkerArguments([...cxx.args, '-shared', '-s', '-o', addonPath, ...objects, importLibrary])
  run('zig', ['lld-link', ...linkerArguments, `-DELAYLOAD:${NODE_HOST_BINARY}`])
}

function link(target, inputs, workDirectory, objects) {
  const addonPath = join(workDirectory, ADDON_FILE_NAME)
  const { cxx } = compilers(target)
  if (target.appleArchitecture) {
    run(cxx.command, [
      ...cxx.args,
      '-bundle',
      '-undefined',
      'dynamic_lookup',
      '-Wl,-dead_strip',
      '-o',
      addonPath,
      ...objects,
    ])
  } else if (target.importMachine) {
    linkWindows(target, inputs, workDirectory, objects, addonPath)
  } else {
    run(cxx.command, [
      ...cxx.args,
      '-shared',
      '-s',
      '-Wl,-Bsymbolic',
      `-Wl,--version-script=${linuxExportsMap}`,
      '-o',
      addonPath,
      ...objects,
      '-ldl',
    ])
  }
  return addonPath
}

function build(name, target) {
  const inputs = resolveInputs()
  const workDirectory = mkdtempSync(join(tmpdir(), 'sirannon-sqlite-'))
  try {
    const sqliteDirectory = prepareSources(inputs, workDirectory)
    const objects = compileObjects(target, inputs, workDirectory, sqliteDirectory)
    const builtPath = link(target, inputs, workDirectory, objects)
    const outputDirectory = join(outputRoot, name)
    mkdirSync(outputDirectory, { recursive: true })
    const outputPath = join(outputDirectory, ADDON_FILE_NAME)
    copyFileSync(builtPath, outputPath)
    return outputPath
  } finally {
    rmSync(workDirectory, { recursive: true, force: true })
  }
}

const requested = process.argv[2] ?? hostTarget()
const target = TARGETS[requested]
if (!target) {
  console.error(
    `No row-counting SQLite build exists for ${requested}. Known targets: ${Object.keys(TARGETS).join(', ')}`,
  )
  process.exit(2)
}

try {
  console.log(`Built ${build(requested, target)}`)
} catch (err) {
  console.error(`Building the row-counting SQLite for ${requested} failed: ${err.message}`)
  process.exit(1)
}
