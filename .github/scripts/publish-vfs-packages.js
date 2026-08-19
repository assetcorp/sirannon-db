import { execFileSync } from 'node:child_process'
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { basename, dirname, join } from 'node:path'

const BUILDS = {
  'darwin-arm64': { package: 'darwin-arm64', library: 'sirannonvfs.dylib' },
  'darwin-x64': { package: 'darwin-x64', library: 'sirannonvfs.dylib' },
  'linux-arm64': { package: 'linux-arm64', library: 'sirannonvfs.so' },
  'linux-arm64-musl': { package: 'linux-arm64', library: join('musl', 'sirannonvfs.so') },
  'linux-x64': { package: 'linux-x64', library: 'sirannonvfs.so' },
  'linux-x64-musl': { package: 'linux-x64', library: join('musl', 'sirannonvfs.so') },
  'win32-arm64': { package: 'win32-arm64', library: 'sirannonvfs.dll' },
  'win32-x64': { package: 'win32-x64', library: 'sirannonvfs.dll' },
}
const PACKAGES = [...new Set(Object.values(BUILDS).map(build => build.package))]

const [version, distTag] = process.argv.slice(2)
if (!version || !distTag) {
  console.error('Usage: publish-vfs-packages.js <version> <dist-tag>')
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

function writeVersion(manifestPath, apply) {
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'))
  apply(manifest)
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`)
}

for (const [build, { package: packageName, library }] of Object.entries(BUILDS)) {
  const built = join('native', 'artifacts', `sirannon-vfs-${build}`, basename(library))
  if (!existsSync(built)) {
    console.error(`The build for ${build} produced no ${basename(library)}`)
    process.exit(1)
  }
  const destination = join('native', 'npm', packageName, library)
  mkdirSync(dirname(destination), { recursive: true })
  copyFileSync(built, destination)
}

let publishedCount = 0
for (const packageName of PACKAGES) {
  const packageDirectory = join('native', 'npm', packageName)
  writeVersion(join(packageDirectory, 'package.json'), manifest => {
    manifest.version = version
  })
  if (alreadyOnTheRegistry(`@delali/sirannon-vfs-${packageName}`)) {
    console.log(`@delali/sirannon-vfs-${packageName}@${version} is already published, so this run left it alone`)
    continue
  }
  execFileSync('npm', ['publish', '--tag', distTag, '--access', 'public'], {
    cwd: packageDirectory,
    stdio: 'inherit',
  })
  publishedCount++
}

writeVersion(join('packages', 'ts', 'package.json'), manifest => {
  manifest.optionalDependencies = {}
  for (const packageName of PACKAGES) {
    manifest.optionalDependencies[`@delali/sirannon-vfs-${packageName}`] = version
  }
})

console.log(
  `Published ${publishedCount} of ${PACKAGES.length} platform packages at ${version} under the ${distTag} tag`,
)
