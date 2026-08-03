const { execFileSync } = require('node:child_process')
const { appendFileSync, readFileSync } = require('node:fs')

const NX_CONFIG = 'nx.json'
const PACKAGE_MANIFEST = 'packages/ts/package.json'

const readJson = path => JSON.parse(readFileSync(path, 'utf8'))

const releaseTagMatcher = pattern => {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new Error(`${NX_CONFIG} must define release.releaseTagPattern`)
  }

  const expanded = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .replace(/\\\{projectName\\\}/g, '\\S+')
    .replace(/\\\{releaseGroupName\\\}/g, '\\S+')
    .replace(/\\\{version\\\}/g, '\\d+\\.\\d+\\.\\d+[0-9A-Za-z.+-]*')

  const unsupported = expanded.match(/\\\{[^}]*\\\}/)
  if (unsupported !== null) {
    throw new Error(`releaseTagPattern uses a placeholder this script cannot expand: ${unsupported[0]}`)
  }

  return new RegExp(`^${expanded}$`)
}

const releaseTagsOn = commit => {
  const listing = execFileSync('git', ['ls-remote', '--tags', 'origin'], { encoding: 'utf8' })
  const matcher = releaseTagMatcher(readJson(NX_CONFIG).release?.releaseTagPattern)
  const tags = []

  for (const line of listing.split('\n')) {
    const [sha, ref] = line.split('\t')
    if (sha !== commit || ref === undefined) continue
    const tag = ref.replace(/^refs\/tags\//, '').replace(/\^\{\}$/, '')
    if (matcher.test(tag)) tags.push(tag)
  }

  return tags
}

const nextPatchVersion = version => {
  const parts = String(version).split('-')[0].split('.').map(Number)
  if (parts.length !== 3 || parts.some(part => !Number.isInteger(part) || part < 0)) {
    throw new Error(`Cannot read a major.minor.patch version from "${version}"`)
  }
  return [parts[0], parts[1], parts[2] + 1].join('.')
}

const writeOutput = (key, value) => {
  const target = process.env.GITHUB_OUTPUT
  if (target === undefined) {
    console.log(`${key}=${value}`)
    return
  }
  appendFileSync(target, `${key}=${value}\n`)
}

const main = () => {
  const commit = process.env.GITHUB_SHA
  const runNumber = process.env.GITHUB_RUN_NUMBER
  if (commit === undefined || runNumber === undefined) {
    throw new Error('Both GITHUB_SHA and GITHUB_RUN_NUMBER must be set')
  }

  const releaseTags = releaseTagsOn(commit)
  if (releaseTags.length > 0) {
    console.log(`${commit} already carries the release tag ${releaseTags[0]}, so it gets no prerelease.`)
    writeOutput('publish', 'false')
    return
  }

  const version = `${nextPatchVersion(readJson(PACKAGE_MANIFEST).version)}-next.${runNumber}`
  console.log(`Publishing ${version} under the next dist-tag.`)
  writeOutput('publish', 'true')
  writeOutput('version', version)
}

main()
