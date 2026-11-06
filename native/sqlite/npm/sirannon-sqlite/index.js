const { existsSync } = require('node:fs')
const { dirname, join, resolve } = require('node:path')
const createDatabase = require('./lib/database')
const SqliteError = require('./lib/sqlite-error')
const { cppdb } = require('./lib/util')

const ADDON_FILE_NAME = 'better_sqlite3.node'
const MUSL_DIRECTORY = 'musl'
const PUBLISHED_TARGETS = new Set([
  'darwin-arm64',
  'darwin-x64',
  'linux-arm64',
  'linux-x64',
  'win32-arm64',
  'win32-x64',
])

let muslLibc

function usesMuslLibc() {
  if (process.platform !== 'linux') return false
  if (muslLibc === undefined) {
    try {
      muslLibc = !process.report.getReport().header.glibcVersionRuntime
    } catch {
      muslLibc = false
    }
  }
  return muslLibc
}

function bindingPath() {
  const target = `${process.platform}-${process.arch}`
  if (!PUBLISHED_TARGETS.has(target)) return null
  let packageDirectory
  try {
    packageDirectory = dirname(require.resolve(`@delali/sirannon-sqlite-${target}/package.json`))
  } catch {
    return null
  }
  const segments = usesMuslLibc() ? [MUSL_DIRECTORY, ADDON_FILE_NAME] : [ADDON_FILE_NAME]
  const addon = join(packageDirectory, ...segments)
  return existsSync(addon) ? addon : null
}

function loadAddon(nativeBinding) {
  if (typeof nativeBinding === 'object' && nativeBinding !== null) return nativeBinding
  const path = typeof nativeBinding === 'string' ? resolve(nativeBinding) : bindingPath()
  if (path === null) {
    const target = `${process.platform}-${process.arch}`
    throw new Error(
      `@delali/sirannon-sqlite found no native build for ${target}. Install @delali/sirannon-sqlite-${target}, or pass the path of a build that you compiled as the nativeBinding option.`,
    )
  }
  return require(path)
}

const Database = createDatabase(loadAddon, true)

Database.prototype.key = function key(rawKey) {
  this[cppdb].key(rawKey)
  return this
}

Database.prototype.startChangeCapture = function startChangeCapture() {
  this[cppdb].startChangeCapture()
  return this
}

Database.prototype.takeCapturedChanges = function takeCapturedChanges() {
  return this[cppdb].takeCapturedChanges()
}

Database.prototype.stopChangeCapture = function stopChangeCapture() {
  this[cppdb].stopChangeCapture()
  return this
}

Database.prototype.applyChanges = function applyChanges(changes) {
  this[cppdb].applyChanges(changes)
  return this
}

module.exports = Database
module.exports.SqliteError = SqliteError
module.exports.bindingPath = bindingPath
