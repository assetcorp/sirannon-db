const { execFileSync } = require('node:child_process')
const { createHash } = require('node:crypto')
const { appendFileSync, existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } = require('node:fs')
const { tmpdir } = require('node:os')
const { join, relative, sep } = require('node:path')

const ARTIFACT_NAME = /^sirannon-(vfs|sqlite)-[a-z0-9-]+$/
const RUN_ID = /^[1-9][0-9]{0,19}$/
const PLATFORM_WORKFLOW = 'publish.yml'
const REPOSITORY = process.env.GITHUB_REPOSITORY ?? ''

function requireArtifactName(name) {
  if (typeof name !== 'string' || !ARTIFACT_NAME.test(name)) {
    throw new Error(`${JSON.stringify(name)} is no native binary artifact`)
  }
}

function digestsBelow(directory) {
  const digests = new Map()
  const visit = current => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const path = join(current, entry.name)
      if (entry.isDirectory()) visit(path)
      else if (entry.isFile()) {
        const name = relative(directory, path).split(sep).join('/')
        digests.set(name, createHash('sha256').update(readFileSync(path)).digest('hex'))
      } else throw new Error(`${path} is neither a file nor a directory`)
    }
  }
  visit(directory)
  return digests
}

function sameBinaries(leftDirectory, rightDirectory) {
  if (!existsSync(leftDirectory) || !existsSync(rightDirectory)) return false
  const left = digestsBelow(leftDirectory)
  const right = digestsBelow(rightDirectory)
  if (left.size === 0 || left.size !== right.size) return false
  return [...left].every(([name, digest]) => right.get(name) === digest)
}

function downloadArtifact(runId, name, directory) {
  if (!RUN_ID.test(String(runId))) throw new Error(`${JSON.stringify(runId)} is no workflow run ID`)
  requireArtifactName(name)
  if (REPOSITORY === '') throw new Error('GITHUB_REPOSITORY is empty')
  execFileSync('gh', ['run', 'download', String(runId), '--repo', REPOSITORY, '--name', name, '--dir', directory], {
    stdio: ['ignore', 'inherit', 'inherit'],
  })
}

function matchesRunArtifact(runId, name, localDirectory) {
  requireArtifactName(name)
  const scratch = mkdtempSync(join(process.env.RUNNER_TEMP || tmpdir(), 'sirannon-binary-match-'))
  try {
    const downloaded = join(scratch, name)
    downloadArtifact(runId, name, downloaded)
    return sameBinaries(localDirectory, downloaded)
  } finally {
    rmSync(scratch, { recursive: true, force: true })
  }
}

function writeOutput(key, value) {
  console.log(`${key}=${value}`)
  const target = process.env.GITHUB_OUTPUT
  if (target !== undefined && target !== '') appendFileSync(target, `${key}=${value}\n`)
}

async function trustedPassingRun(runId) {
  const { request, trustedRun } = require('./proven-suite-runs.js')
  const run = await request(`/repos/${REPOSITORY}/actions/runs/${runId}`)
  return trustedRun(run, PLATFORM_WORKFLOW) && run.status === 'completed' && run.conclusion === 'success'
}

async function selectChecks() {
  const candidates = JSON.parse(process.env.CHECK_MATRIX ?? '')
  const localArtifacts = process.env.LOCAL_ARTIFACTS ?? ''
  const reuseRun = (process.env.REUSE_RUN ?? '').trim()
  if (!Array.isArray(candidates) || candidates.length === 0 || localArtifacts === '') {
    throw new Error('CHECK_MATRIX must list the platform checks and LOCAL_ARTIFACTS must name their binaries')
  }
  for (const candidate of candidates) requireArtifactName(candidate?.artifact)
  let selected = candidates
  if (reuseRun !== '') {
    try {
      if (!RUN_ID.test(reuseRun) || !(await trustedPassingRun(reuseRun))) {
        throw new Error(`run ${reuseRun} is no passing push run of ${PLATFORM_WORKFLOW} on main`)
      }
      selected = candidates.filter(candidate => {
        const matches = matchesRunArtifact(reuseRun, candidate.artifact, join(localArtifacts, candidate.artifact))
        console.log(
          `${candidate.artifact}: ${matches ? `matches run ${reuseRun}, so its check is reused` : 'differs, so its check runs here'}`,
        )
        return !matches
      })
    } catch (error) {
      console.log(`Every platform check runs here, because ${error instanceof Error ? error.message : error}`)
      selected = candidates
    }
  }
  writeOutput('matrix', JSON.stringify(selected.filter(candidate => typeof candidate.runner === 'string')))
  writeOutput('artifacts', JSON.stringify(selected.map(candidate => candidate.artifact)))
}

if (require.main === module) {
  if (process.argv[2] !== '--select') {
    console.error('Usage: native-binary-match.js --select')
    process.exit(2)
  }
  selectChecks().catch(error => {
    console.error(error instanceof Error ? error.message : String(error))
    process.exit(1)
  })
}

module.exports = { matchesRunArtifact }
