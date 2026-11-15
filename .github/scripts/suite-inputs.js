const { execFileSync } = require('node:child_process')
const { createHash } = require('node:crypto')

const INPUT_HASH_FORMAT = 1
const COMMIT = /^[0-9a-f]{40}$/
const RELEASE_CHANGELOG = /(^|\/)CHANGELOG\.md$/
const RELEASED_MANIFEST = 'packages/ts/package.json'
const GIT_OUTPUT_LIMIT_BYTES = 512 * 1024 * 1024

const git = args => execFileSync('git', args, { encoding: 'utf8', maxBuffer: GIT_OUTPUT_LIMIT_BYTES })

function requireCommit(commit) {
  if (typeof commit !== 'string' || !COMMIT.test(commit)) {
    throw new Error(`${JSON.stringify(commit)} is no full commit SHA`)
  }
}

function isAncestor(ancestor, descendant) {
  requireCommit(ancestor)
  requireCommit(descendant)
  try {
    execFileSync('git', ['merge-base', '--is-ancestor', ancestor, descendant], { stdio: 'ignore' })
    return true
  } catch {
    return false
  }
}

function resolvedHead() {
  const head = git(['rev-parse', '--verify', 'HEAD^{commit}']).trim()
  requireCommit(head)
  return head
}

function treeEntries(commit) {
  requireCommit(commit)
  return git(['ls-tree', '-r', '-z', '--full-tree', commit])
    .split('\0')
    .filter(record => record.length > 0)
    .map(record => {
      const tab = record.indexOf('\t')
      const [mode, type, object] = record.slice(0, tab).split(' ')
      return { mode, type, object, path: record.slice(tab + 1) }
    })
}

function versionlessManifest(object, path) {
  const manifest = JSON.parse(git(['cat-file', 'blob', object]))
  if (manifest === null || typeof manifest !== 'object' || Array.isArray(manifest)) {
    throw new Error(`${path} holds no JSON object`)
  }
  const fields = Object.entries(manifest).filter(([key]) => key !== 'version')
  return `versionless ${createHash('sha256').update(JSON.stringify(fields)).digest('hex')}`
}

function createInputHasher() {
  const hashes = new Map()

  return function inputHash(commit) {
    requireCommit(commit)
    const known = hashes.get(commit)
    if (known !== undefined) return known
    const files = treeEntries(commit)
      .filter(entry => !RELEASE_CHANGELOG.test(entry.path))
      .map(entry => {
        const content = entry.path === RELEASED_MANIFEST ? versionlessManifest(entry.object, entry.path) : entry.object
        return `${entry.mode} ${entry.type} ${content}\t${entry.path}`
      })
    if (!files.some(line => line.endsWith(`\t${RELEASED_MANIFEST}`))) {
      throw new Error(`${commit} holds no ${RELEASED_MANIFEST}`)
    }
    const hash = createHash('sha256')
      .update(JSON.stringify({ format: INPUT_HASH_FORMAT, files }))
      .digest('hex')
    hashes.set(commit, hash)
    return hash
  }
}

module.exports = { createInputHasher, isAncestor, requireCommit, resolvedHead }
