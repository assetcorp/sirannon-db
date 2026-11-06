import { execFileSync, spawnSync } from 'node:child_process'
import { copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { referenceRequire, resolveBetterSqliteDirectory } from './better-sqlite3-source.mjs'

const sourceDirectory = dirname(fileURLToPath(import.meta.url))
const outputRoot = join(sourceDirectory, 'out')
const delayLoadHook = join(sourceDirectory, 'win-delay-load-hook.cc')
const linuxExportsMap = join(sourceDirectory, 'linux-exports.map')

const PINNED_ZIG_VERSION = '0.16.0'
const NODE_HOST_BINARY = 'node.exe'
const ADDON_FILE_NAME = 'better_sqlite3.node'
const ZIG_LINK_OUTPUT_LIMIT_BYTES = 64 * 1024 * 1024

export const TARGETS = {
  'darwin-arm64': { appleArchitecture: 'arm64', macosMinimum: '11.0' },
  'darwin-x64': { appleArchitecture: 'x86_64', macosMinimum: '10.15' },
  'linux-arm64': { zigTriple: 'aarch64-linux-gnu' },
  'linux-arm64-musl': { zigTriple: 'aarch64-linux-musl' },
  'linux-x64': { zigTriple: 'x86_64-linux-gnu' },
  'linux-x64-musl': { zigTriple: 'x86_64-linux-musl' },
  'win32-arm64': { zigTriple: 'aarch64-windows-gnu', importMachine: 'arm64' },
  'win32-x64': { zigTriple: 'x86_64-windows-gnu', importMachine: 'i386:x86-64' },
}

const NODE_ADDON_DEFINES = [
  'NAPI_VERSION=10',
  'NAPI_DISABLE_CPP_EXCEPTIONS',
  'NODE_API_SWALLOW_UNTHROWABLE_EXCEPTIONS',
  'BUILDING_NODE_EXTENSION',
  'NODE_GYP_MODULE_NAME=better_sqlite3',
]

export function hostTarget() {
  const pair = `${process.platform}-${process.arch}`
  if (process.platform !== 'linux') return pair
  return process.report.getReport().header.glibcVersionRuntime ? pair : `${pair}-musl`
}

export function betterSqliteDefines(betterSqliteDirectory) {
  const gypi = readFileSync(join(betterSqliteDirectory, 'deps', 'defines.gypi'), 'utf8')
  const defines = [...gypi.matchAll(/^\s*'([A-Z][A-Z0-9_]*(?:=[^']*)?)',?\s*$/gm)].map(match => match[1])
  if (defines.length === 0) {
    throw new Error(`better-sqlite3's deps/defines.gypi lists no SQLite compile options`)
  }
  return defines
}

async function resolveInputs(engine, target) {
  const betterSqliteDirectory = resolveBetterSqliteDirectory()
  const fromBetterSqlite = createRequire(join(betterSqliteDirectory, 'package.json'))
  const nodeApiHeaders = referenceRequire()('node-api-headers')
  return {
    betterSqliteDirectory,
    amalgamationDirectory: await engine.amalgamationDirectory(),
    includeDirectories: await engine.includeDirectories(target),
    nodeAddonApiDirectory: dirname(fromBetterSqlite.resolve('node-addon-api')),
    nodeApiIncludeDirectory: nodeApiHeaders.include_dir,
    nodeApiDefinitions: nodeApiHeaders.def_paths.node_api_def,
  }
}

function run(command, args, options = {}) {
  execFileSync(command, args, { stdio: 'inherit', ...options })
}

function patchesIn(directory) {
  const patches = readdirSync(directory)
    .filter(name => name.endsWith('.patch'))
    .sort()
    .map(name => join(directory, name))
  if (patches.length === 0) {
    throw new Error(`${directory} holds no .patch files, so the build has nothing to apply`)
  }
  return patches
}

function applyPatches(directory, workingDirectory) {
  if (directory === null) return
  for (const patch of patchesIn(directory)) {
    run('git', ['apply', '--whitespace=nowarn', patch], { cwd: workingDirectory })
  }
}

function prepareSources(engine, inputs, workDirectory) {
  const sqliteDirectory = join(workDirectory, 'sqlite3')
  mkdirSync(sqliteDirectory, { recursive: true })
  for (const file of ['sqlite3.c', 'sqlite3.h']) {
    copyFileSync(join(inputs.amalgamationDirectory, file), join(sqliteDirectory, file))
  }
  cpSync(join(inputs.betterSqliteDirectory, 'src'), join(workDirectory, 'src'), { recursive: true })
  applyPatches(engine.sqlitePatchDirectory, sqliteDirectory)
  applyPatches(engine.betterSqlitePatchDirectory, workDirectory)
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

function linkTimeOptimisation(target) {
  return target.importMachine ? [] : ['-flto']
}

function compileObjects(engine, target, inputs, workDirectory, sqliteDirectory) {
  const { c, cxx } = compilers(target)
  const positionIndependent = target.importMachine ? [] : ['-fPIC']
  const sqliteObject = join(workDirectory, 'sqlite3.o')
  run(c.command, [
    ...c.args,
    '-c',
    '-O3',
    ...linkTimeOptimisation(target),
    '-std=c99',
    '-w',
    ...positionIndependent,
    '-DNDEBUG',
    ...engine.sqliteDefines(inputs.betterSqliteDirectory, target).map(define => `-D${define}`),
    '-I',
    sqliteDirectory,
    ...inputs.includeDirectories.flatMap(directory => ['-I', directory]),
    '-o',
    sqliteObject,
    join(sqliteDirectory, 'sqlite3.c'),
  ])
  const addonObject = join(workDirectory, 'better_sqlite3.o')
  run(cxx.command, [
    ...cxx.args,
    '-c',
    '-O3',
    ...linkTimeOptimisation(target),
    '-std=c++20',
    '-fvisibility=hidden',
    '-fvisibility-inlines-hidden',
    ...positionIndependent,
    '-DNDEBUG',
    ...[...engine.addonDefines, ...NODE_ADDON_DEFINES].map(define => `-D${define}`),
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

function nodeImportLibrary(target, definitions, importLibrary) {
  run('zig', ['dlltool', '-d', definitions, '-D', NODE_HOST_BINARY, '-m', target.importMachine, '-l', importLibrary])
  return importLibrary
}

function linkWindows(target, inputs, workDirectory, objects, addonPath) {
  const importLibraries = [nodeImportLibrary(target, inputs.nodeApiDefinitions, join(workDirectory, 'node.lib'))]
  const { cxx } = compilers(target)
  const linkerArguments = zigLinkerArguments([
    ...cxx.args,
    '-shared',
    '-s',
    '-o',
    addonPath,
    ...objects,
    ...importLibraries,
  ])
  run('zig', ['lld-link', ...linkerArguments, `-DELAYLOAD:${NODE_HOST_BINARY}`])
}

function link(engine, target, inputs, workDirectory, objects) {
  const addonPath = join(workDirectory, ADDON_FILE_NAME)
  const { cxx } = compilers(target)
  if (target.appleArchitecture) {
    run(cxx.command, [
      ...cxx.args,
      '-bundle',
      ...linkTimeOptimisation(target),
      '-undefined',
      'dynamic_lookup',
      '-Wl,-dead_strip',
      '-o',
      addonPath,
      ...objects,
      ...engine.appleFrameworks,
    ])
  } else if (target.importMachine) {
    linkWindows(target, inputs, workDirectory, objects, addonPath)
  } else {
    run(cxx.command, [
      ...cxx.args,
      '-shared',
      ...linkTimeOptimisation(target),
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

function assertPinnedZig() {
  const version = execFileSync('zig', ['version'], { encoding: 'utf8' }).trim()
  if (version !== PINNED_ZIG_VERSION) {
    throw new Error(
      `Sirannon's SQLite build needs zig ${PINNED_ZIG_VERSION}, the version that CI pins, and the zig on the PATH is ${version}`,
    )
  }
}

export function resolveTarget(engine, requested) {
  const target = TARGETS[requested]
  if (!target || !engine.supports(target)) {
    const known = Object.entries(TARGETS)
      .filter(([, candidate]) => engine.supports(candidate))
      .map(([name]) => name)
    throw new Error(`${engine.description} has no build target named ${requested}. Known targets: ${known.join(', ')}`)
  }
  return target
}

export async function buildAddon(engine, name, target) {
  if (target.zigTriple) assertPinnedZig()
  const inputs = await resolveInputs(engine, target)
  const workDirectory = mkdtempSync(join(tmpdir(), 'sirannon-sqlite-'))
  try {
    const sqliteDirectory = prepareSources(engine, inputs, workDirectory)
    const objects = compileObjects(engine, target, inputs, workDirectory, sqliteDirectory)
    const builtPath = link(engine, target, inputs, workDirectory, objects)
    const outputDirectory = join(outputRoot, engine.outputDirectoryName(name))
    mkdirSync(outputDirectory, { recursive: true })
    const outputPath = join(outputDirectory, ADDON_FILE_NAME)
    copyFileSync(builtPath, outputPath)
    return outputPath
  } finally {
    rmSync(workDirectory, { recursive: true, force: true })
  }
}
