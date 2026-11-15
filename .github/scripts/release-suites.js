const CI_CALLER = 'CI'
const FAILOVER_CALLER = 'Failover'
const STREAMING_EXTENSION_CALLER = 'Streaming extension'
const SIRANNON_SQLITE_CALLER = 'Sirannon SQLite'
const FAILOVER_PARTS = 2

const parts = (count, name) => Array.from({ length: count }, (_, index) => name(index + 1, count))

const platformTarget = (caller, artifact, job) => ({ artifact, job: `${caller} / ${job}` })

const STREAMING_EXTENSION_TARGETS = [
  ['linux-x64', 'Streamed backup on linux-x64'],
  ['linux-arm64', 'Streamed backup on linux-arm64'],
  ['win32-x64', 'Streamed backup on win32-x64'],
  ['win32-arm64', 'Streamed backup on win32-arm64'],
  ['darwin-arm64', 'Streamed backup on darwin-arm64'],
  ['darwin-x64', 'Streamed backup on darwin-x64'],
  ['linux-x64-musl', 'Streamed backup on linux-x64 musl'],
  ['linux-arm64-musl', 'Streamed backup on linux-arm64 musl'],
].map(([target, job]) => platformTarget(STREAMING_EXTENSION_CALLER, `sirannon-vfs-${target}`, job))

const SIRANNON_SQLITE_TARGETS = [
  'linux-x64',
  'linux-arm64',
  'win32-x64',
  'win32-arm64',
  'darwin-arm64',
  'darwin-x64',
  'linux-arm64-musl',
  'linux-x64-musl',
].map(target => platformTarget(SIRANNON_SQLITE_CALLER, `sirannon-sqlite-${target}`, `Check on ${target}`))

const ciSuite = jobs => ({ workflow: 'ci.yml', jobs, releaseJobs: jobs.map(job => `${CI_CALLER} / ${job}`) })

const platformSuite = targets => ({
  workflow: 'publish.yml',
  jobs: targets.map(target => target.job),
  releaseJobs: targets.map(target => target.job),
  targets,
})

const FAILOVER_JOBS = parts(FAILOVER_PARTS, (part, count) => `Coordinator failover (Node 24), part ${part} of ${count}`)

const SUITES = {
  'ci-node': ciSuite(['Node 22', 'Node 24']),
  'ci-e2e': ciSuite(['Replication E2E (Node 22)', 'Replication E2E (Node 24)']),
  'ci-large-backups': ciSuite(['Backups past the pending byte page']),
  'ci-benchmarks': ciSuite(['Benchmarks writeup drift', 'Benchmarks harness tests']),
  failover: {
    workflow: 'failover.yml',
    jobs: FAILOVER_JOBS,
    releaseJobs: FAILOVER_JOBS.map(job => `${FAILOVER_CALLER} / ${job}`),
  },
  'vfs-platform': platformSuite(STREAMING_EXTENSION_TARGETS),
  'sqlite-platform': platformSuite(SIRANNON_SQLITE_TARGETS),
}

module.exports = { SUITES }
