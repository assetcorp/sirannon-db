const { execFileSync } = require('node:child_process')
const { appendFileSync } = require('node:fs')

const COMMIT = /^[0-9a-f]{40}$/
const MISSING_COMMIT = /^0{40}$/
const RELEASE_CHANGELOG = 'CHANGELOG.md'
const RELEASED_MANIFEST = 'packages/ts/package.json'
const GIT_OUTPUT_LIMIT_BYTES = 64 * 1024 * 1024

const SHARED_PLATFORM_CHECK_INPUTS = [
  'native/',
  'packages/ts/',
  'pnpm-lock.yaml',
  'package.json',
  'pnpm-workspace.yaml',
  '.npmrc',
  'nx.json',
  'biome.json',
  'tsconfig.json',
  '.github/actions/',
  '.github/scripts/ci-scope.js',
  '.github/scripts/native-build-key.mjs',
  '.github/scripts/native-binary-match.js',
  '.github/scripts/proven-suite-runs.js',
  '.github/scripts/suite-inputs.js',
]

const UNTESTED_BY_PLATFORM_CHECKS = ['packages/ts/src/__tests__/failover/', 'packages/ts/src/__tests__/large/']

const PLATFORM_CHECKS = {
  vfs: {
    paths: [...SHARED_PLATFORM_CHECK_INPUTS, '.github/workflows/streaming-extension.yml'],
    except: [...UNTESTED_BY_PLATFORM_CHECKS, 'packages/ts/src/__tests__/e2e/'],
  },
  sqlite: {
    paths: [...SHARED_PLATFORM_CHECK_INPUTS, '.github/workflows/sirannon-sqlite.yml'],
    except: UNTESTED_BY_PLATFORM_CHECKS,
  },
}

const git = args => execFileSync('git', args, { encoding: 'utf8', maxBuffer: GIT_OUTPUT_LIMIT_BYTES })

function requireCommit(name, commit) {
  if (typeof commit !== 'string' || !COMMIT.test(commit) || MISSING_COMMIT.test(commit)) {
    throw new Error(`${name} holds no commit SHA`)
  }
}

function changedFiles(from, to) {
  return git(['diff', '--no-renames', '--name-only', '-z', from, to])
    .split('\0')
    .filter(path => path.length > 0)
}

function writeOutput(key, value) {
  console.log(`${key}=${value}`)
  const target = process.env.GITHUB_OUTPUT
  if (target !== undefined && target !== '') appendFileSync(target, `${key}=${value}\n`)
}

function quoted(paths) {
  return paths.map(path => JSON.stringify(path).replaceAll('##[', '##\\u005b')).join('\n')
}

function versionless(commit) {
  const manifest = JSON.parse(git(['show', `${commit}:${RELEASED_MANIFEST}`]))
  if (manifest === null || typeof manifest !== 'object' || Array.isArray(manifest)) {
    throw new Error(`${RELEASED_MANIFEST} holds no JSON object at ${commit}`)
  }
  return JSON.stringify(Object.entries(manifest).filter(([key]) => key !== 'version'))
}

function isReleaseOnly(before, after) {
  requireCommit('BEFORE', before)
  requireCommit('AFTER', after)
  execFileSync('git', ['merge-base', '--is-ancestor', before, after], { stdio: 'ignore' })
  const files = changedFiles(before, after)
  console.log(`Changed files:\n${quoted(files)}`)
  if (files.length === 0) return false
  if (!files.every(path => path === RELEASE_CHANGELOG || path === RELEASED_MANIFEST)) return false
  return !files.includes(RELEASED_MANIFEST) || versionless(before) === versionless(after)
}

function needsPlatformChecks(binary, base, head) {
  const scope = PLATFORM_CHECKS[binary]
  if (scope === undefined) throw new Error(`no platform checks are called ${JSON.stringify(binary)}`)
  requireCommit('BASE', base)
  requireCommit('HEAD', head)
  const mergeBase = git(['merge-base', base, head]).trim()
  const files = changedFiles(mergeBase, head)
  console.log(`Changed files:\n${quoted(files)}`)
  return files.some(
    path =>
      scope.paths.some(prefix => path === prefix || (prefix.endsWith('/') && path.startsWith(prefix))) &&
      !scope.except.some(prefix => path.startsWith(prefix)) &&
      !(path.startsWith('packages/ts/') && path.endsWith('.md')),
  )
}

function main() {
  const [mode, binary] = process.argv.slice(2)
  if (mode === '--release-only') {
    let releaseOnly = false
    try {
      releaseOnly = isReleaseOnly(process.env.BEFORE, process.env.AFTER)
    } catch (error) {
      console.log(`Treating this push as a full change, because ${error instanceof Error ? error.message : error}`)
    }
    writeOutput('release_only', String(releaseOnly))
    return
  }
  if (mode === '--platform-checks' && binary !== undefined) {
    if (process.env.EVENT_NAME !== 'pull_request') {
      writeOutput('run', 'true')
      return
    }
    let run = true
    try {
      run = needsPlatformChecks(binary, process.env.BASE, process.env.HEAD)
    } catch (error) {
      console.log(`Running every platform check, because ${error instanceof Error ? error.message : error}`)
    }
    writeOutput('run', String(run))
    return
  }
  console.error('Usage: ci-scope.js --release-only | --platform-checks <vfs|sqlite>')
  process.exit(2)
}

main()
