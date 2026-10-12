import { execFileSync } from 'node:child_process'
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { copyBetterSqliteJavaScript } from '../../native/sqlite/better-sqlite3-source.mjs'

const ADDON_FILE_NAME = 'better_sqlite3.node'
const ROW_COUNTING_PACKAGE = '@delali/sirannon-sqlite'
const BUILDS = {
  'darwin-arm64': { package: 'darwin-arm64', addon: ADDON_FILE_NAME },
  'darwin-x64': { package: 'darwin-x64', addon: ADDON_FILE_NAME },
  'linux-arm64': { package: 'linux-arm64', addon: ADDON_FILE_NAME },
  'linux-arm64-musl': { package: 'linux-arm64', addon: join('musl', ADDON_FILE_NAME) },
  'linux-x64': { package: 'linux-x64', addon: ADDON_FILE_NAME },
  'linux-x64-musl': { package: 'linux-x64', addon: join('musl', ADDON_FILE_NAME) },
  'win32-arm64': { package: 'win32-arm64', addon: ADDON_FILE_NAME },
  'win32-x64': { package: 'win32-x64', addon: ADDON_FILE_NAME },
}
const PLATFORM_PACKAGES = [...new Set(Object.values(BUILDS).map(build => build.package))]
const PACKAGE_ROOT = join('native', 'sqlite', 'npm')
const MAIN_PACKAGE_DIRECTORY = join(PACKAGE_ROOT, 'sirannon-sqlite')
const LICENCE_FILES = ['LICENSE', 'NOTICE']

const [version, distTag] = process.argv.slice(2)
if (!version || !distTag) {
  console.error('Usage: publish-sqlite-packages.js <version> <dist-tag>')
  process.exit(2)
}

function alreadyOnTheRegistry(packageName) {
  try {
    const published = execFileSync('npm', ['view', `${packageName}@${version}`, 'version'], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    })
    return published.trim() === version
  } catch {
    return false
  }
}

function rewriteManifest(manifestPath, apply) {
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'))
  apply(manifest)
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`)
}

function publish(packageName, packageDirectory) {
  if (alreadyOnTheRegistry(packageName)) {
    console.log(`${packageName}@${version} is already published, so this run left it alone`)
    return 'skipped'
  }
  try {
    execFileSync('npm', ['publish', '--tag', distTag, '--access', 'public'], {
      cwd: packageDirectory,
      stdio: 'inherit',
    })
    return 'published'
  } catch {
    return 'refused'
  }
}

function reportRefusals(refused, total) {
  if (refused.length === 0) return
  console.error(
    `The registry refused ${refused.length} of ${total} row-counting SQLite packages at ${version}: ${refused.join(', ')}. ` +
      'A package that exists and answers 404 to a publish has no trusted publisher for this workflow, ' +
      'which npm configures with: npm trust github <package> --file publish.yml --repo assetcorp/sirannon-db',
  )
  process.exit(1)
}

for (const [build, { package: packageName, addon }] of Object.entries(BUILDS)) {
  const built = join('native', 'artifacts', `sirannon-sqlite-${build}`, ADDON_FILE_NAME)
  if (!existsSync(built)) {
    console.error(`The build for ${build} produced no ${ADDON_FILE_NAME}`)
    process.exit(1)
  }
  const destination = join(PACKAGE_ROOT, packageName, addon)
  mkdirSync(dirname(destination), { recursive: true })
  copyFileSync(built, destination)
}

copyBetterSqliteJavaScript()
for (const packageDirectory of [MAIN_PACKAGE_DIRECTORY, ...PLATFORM_PACKAGES.map(name => join(PACKAGE_ROOT, name))]) {
  for (const file of LICENCE_FILES) copyFileSync(file, join(packageDirectory, file))
}

let publishedCount = 0
const refused = []
for (const packageName of PLATFORM_PACKAGES) {
  const packageDirectory = join(PACKAGE_ROOT, packageName)
  rewriteManifest(join(packageDirectory, 'package.json'), manifest => {
    manifest.version = version
  })
  const outcome = publish(`${ROW_COUNTING_PACKAGE}-${packageName}`, packageDirectory)
  if (outcome === 'published') publishedCount++
  if (outcome === 'refused') refused.push(`${ROW_COUNTING_PACKAGE}-${packageName}`)
}
reportRefusals(refused, PLATFORM_PACKAGES.length + 1)

rewriteManifest(join(MAIN_PACKAGE_DIRECTORY, 'package.json'), manifest => {
  manifest.version = version
  manifest.optionalDependencies = {}
  for (const packageName of PLATFORM_PACKAGES) {
    manifest.optionalDependencies[`${ROW_COUNTING_PACKAGE}-${packageName}`] = version
  }
})
const mainOutcome = publish(ROW_COUNTING_PACKAGE, MAIN_PACKAGE_DIRECTORY)
if (mainOutcome === 'published') publishedCount++
reportRefusals(mainOutcome === 'refused' ? [ROW_COUNTING_PACKAGE] : [], PLATFORM_PACKAGES.length + 1)

rewriteManifest(join('packages', 'ts', 'package.json'), manifest => {
  manifest.peerDependencies[ROW_COUNTING_PACKAGE] = version
  manifest.devDependencies[ROW_COUNTING_PACKAGE] = version
})

console.log(
  `Published ${publishedCount} of ${PLATFORM_PACKAGES.length + 1} row-counting SQLite packages at ${version} under the ${distTag} tag`,
)
