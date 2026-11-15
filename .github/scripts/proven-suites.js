const { appendFileSync } = require('node:fs')
const { join } = require('node:path')
const { matchesRunArtifact } = require('./native-binary-match.js')
const { createRunLookup, currentRunJobs } = require('./proven-suite-runs.js')
const { SUITES } = require('./release-suites.js')
const { createInputHasher, resolvedHead } = require('./suite-inputs.js')

const SUCCESS_OR_SKIPPED = new Set(['success', 'skipped'])
const CI_WORKFLOW = 'ci.yml'
const NO_SUITES = 'none'
const REPOSITORY = process.env.GITHUB_REPOSITORY ?? ''

const outputKey = suiteName => suiteName.replaceAll('-', '_')

function writeOutput(key, value) {
  console.log(`${key}=${value}`)
  const target = process.env.GITHUB_OUTPUT
  if (target !== undefined && target !== '') appendFileSync(target, `${key}=${value}\n`)
}

function writeSummary(markdown) {
  const target = process.env.GITHUB_STEP_SUMMARY
  if (target !== undefined && target !== '') appendFileSync(target, markdown)
}

function verdictOf(requiredJobs, jobs) {
  const conclusions = new Map()
  for (const job of jobs) {
    if (!requiredJobs.includes(job.name)) continue
    const known = conclusions.get(job.name) ?? []
    conclusions.set(job.name, [...known, job.status === 'completed' ? job.conclusion : job.status])
  }
  const reported = [...conclusions.values()].flat()
  if (reported.some(conclusion => !SUCCESS_OR_SKIPPED.has(conclusion))) return 'failed'
  const everyJobPassed = requiredJobs.every(name =>
    conclusions.get(name)?.every(conclusion => conclusion === 'success'),
  )
  return everyJobPassed ? 'passed' : 'not-run'
}

function newestFirst(left, right) {
  const byCreation = Date.parse(right.created_at) - Date.parse(left.created_at)
  return byCreation !== 0 ? byCreation : right.id - left.id
}

async function proofOf(suiteName, head, lookup) {
  const suite = SUITES[suiteName]
  const releaseHash = lookup.inputHash(head)
  const runs = [...(await lookup.runsOf(suite.workflow))].sort(newestFirst)
  const testsTheSameInputs = run => {
    try {
      return lookup.inputHash(run.head_sha) === releaseHash
    } catch {
      return false
    }
  }
  const matching = []
  for (const run of runs) {
    if (!testsTheSameInputs(run)) continue
    if (run.status !== 'completed') {
      return { proven: false, reason: `${run.html_url} is testing the same inputs and has not finished` }
    }
    matching.push(run)
  }
  for (const run of matching) {
    const verdict = verdictOf(suite.jobs, await lookup.jobsOf(run))
    if (verdict === 'not-run') continue
    if (verdict === 'failed') return { proven: false, reason: `${run.html_url} failed this suite on the same inputs` }
    if (run.conclusion !== 'success') continue
    await lookup.requireUnchanged(suite.workflow, run, testsTheSameInputs)
    return { proven: true, run }
  }
  return { proven: false, reason: 'no completed push run on main passed this suite on the same inputs' }
}

async function settledProofOf(suiteName, head, lookup) {
  try {
    return await proofOf(suiteName, head, lookup)
  } catch (error) {
    return { proven: false, reason: `the lookup failed: ${error instanceof Error ? error.message : error}` }
  }
}

function createLookup(head) {
  return { inputHash: createInputHasher(), ...createRunLookup(head) }
}

function summaryRow(suiteName, proof) {
  if (!proof.proven) return `| \`${suiteName}\` | runs in this release | ${proof.reason} |`
  const commit = proof.run.head_sha.slice(0, 9)
  return `| \`${suiteName}\` | reused | [run ${proof.run.id}](${proof.run.html_url}) passed it on \`${commit}\` |`
}

async function lookUp() {
  const head = resolvedHead()
  const lookup = createLookup(head)
  const proofs = new Map()
  for (const suiteName of Object.keys(SUITES)) proofs.set(suiteName, await settledProofOf(suiteName, head, lookup))
  const rows = [
    '## Suites that main has already passed on these inputs',
    '',
    '| Suite | Result | Evidence |',
    '|---|---|---|',
  ]
  for (const [suiteName, proof] of proofs) rows.push(summaryRow(suiteName, proof))
  console.log(rows.join('\n'))
  writeSummary(`${rows.join('\n')}\n\n`)
  const proven = [...proofs].filter(([, proof]) => proof.proven).map(([name]) => name)
  const unproven = [...proofs].filter(([, proof]) => !proof.proven).map(([name]) => name)
  for (const [suiteName, proof] of proofs) {
    writeOutput(outputKey(suiteName), String(proof.proven))
    if (SUITES[suiteName].targets !== undefined) {
      writeOutput(`${outputKey(suiteName)}_run`, proof.proven ? String(proof.run.id) : '')
    }
  }
  const unprovenCi = unproven.filter(name => SUITES[name].workflow === CI_WORKFLOW)
  writeOutput('proven', proven.join(','))
  writeOutput('unproven', unproven.join(','))
  writeOutput('ci_suites', unprovenCi.length > 0 ? unprovenCi.join(',') : NO_SUITES)
}

function passedHere(jobs, name) {
  const conclusions = jobs.filter(job => job.name === name).map(job => job.conclusion)
  return conclusions.length > 0 && conclusions.every(conclusion => conclusion === 'success')
}

function parsedSuiteList(text) {
  const names = text
    .split(',')
    .map(name => name.trim())
    .filter(name => name.length > 0)
  const unknown = names.filter(name => SUITES[name] === undefined)
  if (unknown.length > 0) throw new Error(`no suite is called ${unknown.join(', ')}`)
  return new Set(names)
}

async function gateSuite(suiteName, context) {
  const suite = SUITES[suiteName]
  const unchecked = (suite.targets ?? suite.releaseJobs.map(job => ({ job }))).filter(
    target => !passedHere(context.jobs, target.job),
  )
  if (unchecked.length === 0) return `| \`${suiteName}\` | passed in this run |`
  if (!context.reused.has(suiteName)) {
    throw new Error(
      `${suiteName} had to pass in this run, and ${unchecked.map(target => target.job).join(', ')} did not`,
    )
  }
  const proof = await settledProofOf(suiteName, context.head, context.lookup)
  if (!proof.proven) throw new Error(`main no longer proves ${suiteName} on these inputs: ${proof.reason}`)
  if (suite.targets !== undefined) {
    for (const target of unchecked) {
      const local = join(context.artifacts, target.artifact)
      if (!matchesRunArtifact(proof.run.id, target.artifact, local)) {
        throw new Error(
          `${target.artifact} differs from the binary that run ${proof.run.id} checked, and no check ran here`,
        )
      }
    }
  }
  return summaryRow(suiteName, proof)
}

async function gate() {
  const artifacts = process.env.NATIVE_ARTIFACTS ?? ''
  if (artifacts === '') throw new Error('NATIVE_ARTIFACTS must name the directory that holds the binaries to publish')
  const head = resolvedHead()
  const context = {
    head,
    artifacts,
    jobs: await currentRunJobs(),
    lookup: createLookup(head),
    reused: parsedSuiteList(process.env.REUSED_SUITES ?? ''),
  }
  const rows = []
  const failures = []
  for (const suiteName of Object.keys(SUITES)) {
    try {
      rows.push(await gateSuite(suiteName, context))
    } catch (error) {
      failures.push(error instanceof Error ? error.message : String(error))
    }
  }
  console.log(rows.join('\n'))
  if (failures.length > 0) throw new Error(`This release publishes nothing, because ${failures.join('; ')}`)
}

async function main() {
  if (REPOSITORY === '') throw new Error('GITHUB_REPOSITORY is empty')
  const mode = process.argv[2]
  if (mode === '--lookup') return lookUp()
  if (mode === '--gate') return gate()
  throw new Error('Usage: proven-suites.js --lookup | --gate')
}

main().catch(error => {
  console.error(error instanceof Error ? error.message : String(error))
  process.exit(1)
})
