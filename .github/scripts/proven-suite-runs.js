const { isAncestor, requireCommit } = require('./suite-inputs.js')

const MAIN_BRANCH = 'main'
const PER_PAGE = 100
const NEWEST_RUN_PAGES = 3
const MAX_BLOCKING_RUN_PAGES = 10
const BLOCKING_RUN_STATES = [
  'in_progress',
  'queued',
  'requested',
  'waiting',
  'pending',
  'action_required',
  'cancelled',
  'failure',
  'neutral',
  'skipped',
  'stale',
  'timed_out',
]
const KNOWN_RUN_STATUSES = ['completed', 'in_progress', 'queued', 'requested', 'waiting', 'pending']
const MAX_JOB_PAGES = 5
const REQUEST_TIMEOUT_MS = 30_000
const REQUEST_ATTEMPTS = 3
const RETRY_DELAY_MS = 2_000
const LISTING_ATTEMPTS = 4
const LISTING_RETRY_DELAY_MS = 5_000
const API = process.env.GITHUB_API_URL ?? 'https://api.github.com'
const REPOSITORY = process.env.GITHUB_REPOSITORY ?? ''

async function request(path) {
  const token = process.env.GITHUB_TOKEN ?? ''
  if (token === '') throw new Error('GITHUB_TOKEN is empty')
  const headers = {
    Accept: 'application/vnd.github+json',
    Authorization: `Bearer ${token}`,
    'X-GitHub-Api-Version': '2022-11-28',
  }
  for (let attempt = 1; ; attempt += 1) {
    let response = null
    let failure = null
    try {
      response = await fetch(`${API}${path}`, { headers, signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) })
    } catch (error) {
      failure = error instanceof Error ? error.message : String(error)
    }
    if (response?.ok) return response.json()
    const retryable = response === null || response.status >= 500 || response.status === 429
    if (!retryable || attempt >= REQUEST_ATTEMPTS) {
      throw new Error(`the GitHub API answered ${response?.status ?? failure} to ${path}`)
    }
    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * attempt))
  }
}

class ListingDisagreement extends Error {}

async function pagesOnce(path, field, limit, newestOnly) {
  const entries = []
  const ids = new Set()
  let total = null
  for (let page = 1; page <= limit; page += 1) {
    const separator = path.includes('?') ? '&' : '?'
    const body = await request(`${path}${separator}per_page=${PER_PAGE}&page=${page}`)
    if (!Array.isArray(body?.[field]) || !Number.isSafeInteger(body.total_count) || body.total_count < 0) {
      throw new Error(`the GitHub API returned an invalid ${field} page`)
    }
    if (total !== null && total !== body.total_count) {
      throw new ListingDisagreement(`${field} changed during pagination`)
    }
    total = body.total_count
    if (!newestOnly && total > limit * PER_PAGE) throw new Error(`${field} exceeds the lookup limit`)
    for (const entry of body[field]) {
      if (!Number.isSafeInteger(entry?.id) || entry.id <= 0) throw new Error(`${field} holds an invalid ID`)
      if (ids.has(entry.id)) throw new ListingDisagreement(`${field} holds a repeated ID`)
      ids.add(entry.id)
      entries.push(entry)
    }
    if (entries.length > total) throw new ListingDisagreement(`${field} lists more entries than it counts`)
    if (entries.length === total) return entries
    if (body[field].length !== PER_PAGE) throw new ListingDisagreement(`${field} lists fewer entries than it counts`)
  }
  if (newestOnly) return entries
  throw new Error(`${field} exceeds the lookup limit`)
}

async function pagesOf(path, field, limit, { newestOnly = false } = {}) {
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await pagesOnce(path, field, limit, newestOnly)
    } catch (error) {
      if (!(error instanceof ListingDisagreement) || attempt >= LISTING_ATTEMPTS) throw error
      await new Promise(resolve => setTimeout(resolve, LISTING_RETRY_DELAY_MS * attempt))
    }
  }
}

function trustedRun(run, workflow) {
  requireCommit(run.head_sha)
  if (
    !Number.isSafeInteger(run.run_attempt) ||
    run.run_attempt <= 0 ||
    !KNOWN_RUN_STATUSES.includes(run.status) ||
    !Number.isFinite(Date.parse(run.updated_at)) ||
    !Number.isFinite(Date.parse(run.created_at)) ||
    typeof run.event !== 'string' ||
    typeof run.head_branch !== 'string' ||
    typeof run.path !== 'string' ||
    (run.status === 'completed' && typeof run.conclusion !== 'string') ||
    typeof run.repository?.full_name !== 'string' ||
    typeof run.head_repository?.full_name !== 'string'
  ) {
    throw new Error(`run ${run.id} holds incomplete metadata`)
  }
  return (
    run.event === 'push' &&
    run.head_branch === MAIN_BRANCH &&
    run.path === `.github/workflows/${workflow}` &&
    run.repository.full_name === REPOSITORY &&
    run.head_repository.full_name === REPOSITORY
  )
}

function runState(run) {
  return JSON.stringify([
    run.id,
    run.head_sha,
    run.run_attempt,
    run.status,
    run.conclusion,
    run.updated_at,
    run.path,
    run.event,
    run.head_branch,
    run.repository?.full_name,
    run.head_repository?.full_name,
  ])
}

function requireJobsOf(jobs, runId, runAttempt, headSha) {
  const foreign = jobs.some(
    job =>
      job.run_id !== runId ||
      job.run_attempt !== runAttempt ||
      (headSha !== null && job.head_sha !== headSha) ||
      typeof job.name !== 'string' ||
      typeof job.status !== 'string',
  )
  if (foreign) throw new Error(`run ${runId} lists jobs from another attempt or commit`)
  return jobs
}

async function attemptJobs(runId, runAttempt, headSha) {
  const path = `/repos/${REPOSITORY}/actions/runs/${runId}/attempts/${runAttempt}/jobs`
  return requireJobsOf(await pagesOf(path, 'jobs', MAX_JOB_PAGES), runId, runAttempt, headSha)
}

async function currentRunJobs() {
  const runId = Number(process.env.GITHUB_RUN_ID)
  const runAttempt = Number(process.env.GITHUB_RUN_ATTEMPT)
  if (!Number.isSafeInteger(runId) || runId <= 0 || !Number.isSafeInteger(runAttempt) || runAttempt <= 0) {
    throw new Error('GITHUB_RUN_ID and GITHUB_RUN_ATTEMPT must name this run')
  }
  return attemptJobs(runId, runAttempt, process.env.GITHUB_SHA ?? null)
}

function createRunLookup(head) {
  const runsByWorkflow = new Map()
  const jobsByAttempt = new Map()

  async function listRuns(workflow) {
    const path = `/repos/${REPOSITORY}/actions/workflows/${workflow}/runs?branch=${MAIN_BRANCH}&event=push`
    const runs = await pagesOf(path, 'workflow_runs', NEWEST_RUN_PAGES, { newestOnly: true })
    const blocking = await Promise.all(
      BLOCKING_RUN_STATES.map(async status => {
        const entries = await pagesOf(`${path}&status=${status}`, 'workflow_runs', MAX_BLOCKING_RUN_PAGES)
        if (entries.some(run => run.status !== status && run.conclusion !== status)) {
          throw new Error(`${workflow} lists a run outside its ${status} filter`)
        }
        return entries
      }),
    )
    const byId = new Map(runs.map(run => [run.id, run]))
    for (const run of blocking.flat()) byId.set(run.id, run)
    return [...byId.values()].filter(run => trustedRun(run, workflow))
  }

  async function runsOf(workflow) {
    if (!runsByWorkflow.has(workflow)) runsByWorkflow.set(workflow, listRuns(workflow))
    return (await runsByWorkflow.get(workflow)).filter(run => isAncestor(run.head_sha, head))
  }

  async function jobsOf(run) {
    const current = await request(`/repos/${REPOSITORY}/actions/runs/${run.id}`)
    if (runState(current) !== runState(run)) throw new Error(`run ${run.id} changed before its jobs were read`)
    const key = `${run.id}/${run.run_attempt}`
    if (!jobsByAttempt.has(key)) jobsByAttempt.set(key, attemptJobs(run.id, run.run_attempt, run.head_sha))
    return jobsByAttempt.get(key)
  }

  async function requireUnchanged(workflow, run, testsTheSameInputs) {
    const statesBefore = new Map(
      (await runsOf(workflow)).filter(testsTheSameInputs).map(entry => [entry.id, runState(entry)]),
    )
    const runsAfter = (await listRuns(workflow)).filter(entry => isAncestor(entry.head_sha, head))
    for (const entry of runsAfter.filter(testsTheSameInputs)) {
      if (statesBefore.get(entry.id) !== runState(entry)) {
        throw new Error(
          `${entry.html_url} started or changed while this release read the runs that test the same inputs`,
        )
      }
      statesBefore.delete(entry.id)
    }
    await Promise.all(
      [...statesBefore].map(async ([id, state]) => {
        const current = await request(`/repos/${REPOSITORY}/actions/runs/${id}`)
        if (runState(current) !== state) throw new Error(`run ${id} changed after leaving the lookup window`)
      }),
    )
    const current = await request(`/repos/${REPOSITORY}/actions/runs/${run.id}`)
    if (runState(current) !== runState(run)) throw new Error(`run ${run.id} changed after its jobs were read`)
  }

  return { runsOf, jobsOf, requireUnchanged }
}

module.exports = { createRunLookup, currentRunJobs, request, trustedRun }
