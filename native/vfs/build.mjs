import { execFileSync } from 'node:child_process'
import { mkdirSync, readdirSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const sourceDirectory = dirname(fileURLToPath(import.meta.url))
const packagesDirectory = join(sourceDirectory, '..', 'npm')
const referenceManifest = join(sourceDirectory, '..', '..', 'packages', 'ts', 'package.json')

const TARGETS = {
  'darwin-arm64': { package: 'darwin-arm64', library: 'sirannonvfs.dylib', appleArchitecture: 'arm64' },
  'darwin-x64': { package: 'darwin-x64', library: 'sirannonvfs.dylib', appleArchitecture: 'x86_64' },
  'linux-arm64': { package: 'linux-arm64', library: 'sirannonvfs.so', zigTriple: 'aarch64-linux-gnu' },
  'linux-arm64-musl': {
    package: 'linux-arm64',
    libcDirectory: 'musl',
    library: 'sirannonvfs.so',
    zigTriple: 'aarch64-linux-musl',
  },
  'linux-x64': { package: 'linux-x64', library: 'sirannonvfs.so', zigTriple: 'x86_64-linux-gnu' },
  'linux-x64-musl': {
    package: 'linux-x64',
    libcDirectory: 'musl',
    library: 'sirannonvfs.so',
    zigTriple: 'x86_64-linux-musl',
  },
  'win32-arm64': { package: 'win32-arm64', library: 'sirannonvfs.dll', zigTriple: 'aarch64-windows-gnu' },
  'win32-x64': { package: 'win32-x64', library: 'sirannonvfs.dll', zigTriple: 'x86_64-windows-gnu' },
}

function hostTarget() {
  const pair = `${process.platform}-${process.arch}`
  if (process.platform !== 'linux') return pair
  return process.report.getReport().header.glibcVersionRuntime ? pair : `${pair}-musl`
}

function sqliteHeaderDirectory() {
  const require = createRequire(pathToFileURL(referenceManifest))
  return join(dirname(require.resolve('better-sqlite3/package.json')), 'deps', 'sqlite3')
}

function sourceFiles() {
  return readdirSync(sourceDirectory)
    .filter(name => name.endsWith('.c'))
    .sort()
    .map(name => join(sourceDirectory, name))
}

function commandFor(target, outputPath) {
  const positionIndependent = target.zigTriple?.includes('windows') ? [] : ['-fPIC']
  const shared = ['-O2', '-shared', ...positionIndependent, '-I', sqliteHeaderDirectory(), '-o', outputPath, ...sourceFiles()]
  if (target.appleArchitecture) {
    return { command: 'cc', args: ['-arch', target.appleArchitecture, ...shared] }
  }
  return { command: 'zig', args: ['cc', '-target', target.zigTriple, ...shared] }
}

const requested = process.argv[2] ?? hostTarget()
const target = TARGETS[requested]
if (!target) {
  console.error(
    `No streaming extension is published for ${requested}. Known targets: ${Object.keys(TARGETS).join(', ')}`,
  )
  process.exit(2)
}

const outputDirectory = join(packagesDirectory, target.package, target.libcDirectory ?? '')
mkdirSync(outputDirectory, { recursive: true })
const outputPath = join(outputDirectory, target.library)
const { command, args } = commandFor(target, outputPath)

try {
  execFileSync(command, args, { stdio: 'inherit' })
} catch (err) {
  console.error(`Building the streaming extension for ${requested} failed: ${err.message}`)
  process.exit(1)
}

console.log(`Built ${outputPath}`)
