import { execSync, spawnSync } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { collectSystemInfo } from './config'

const COMPOSE_FILE = join(import.meta.dirname, 'docker', 'docker-compose.yml')
const RESULTS_DIR = join(import.meta.dirname, 'results')
const K6_DIR = join(import.meta.dirname, 'k6')

const SIRANNON_URL = process.env.SIRANNON_URL ?? 'http://localhost:9876'
const POSTGRES_URL = process.env.POSTGRES_URL ?? 'http://localhost:9877'
const DATA_SIZE = process.env.BENCH_DATA_SIZE ?? '10000'

const RPS_LEVELS = (process.env.BENCH_RPS_LEVELS ?? '1000,5000,10000').split(',').map(Number)
const DURATION = process.env.BENCH_DURATION ?? '60s'

interface K6Script {
  name: string
  file: string
}

const scripts: K6Script[] = [
  { name: 'point-select', file: 'point-select.js' },
  { name: 'mixed-readwrite', file: 'mixed-readwrite.js' },
  { name: 'transaction', file: 'transaction.js' },
]

function run(cmd: string, opts?: { cwd?: string; ignoreError?: boolean }) {
  console.log(`> ${cmd}`)
  try {
    execSync(cmd, { stdio: 'inherit', cwd: opts?.cwd })
  } catch (err) {
    if (!opts?.ignoreError) throw err
  }
}

function checkK6Installed(): boolean {
  try {
    execSync('k6 version', { stdio: 'pipe' })
    return true
  } catch {
    return false
  }
}

async function waitForHealth(url: string, label: string, maxRetries = 30) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const res = await fetch(`${url}/health`)
      if (res.ok) {
        console.log(`${label} is healthy`)
        return
      }
    } catch {}
    await new Promise(r => setTimeout(r, 2000))
  }
  throw new Error(`${label} did not become healthy after ${maxRetries * 2}s`)
}

function runK6(script: K6Script, targetRps: number): string | null {
  const scriptPath = join(K6_DIR, script.file)
  const summaryFile = join(RESULTS_DIR, `e2e-${script.name}-${targetRps}rps.json`)

  console.log(`\nRunning ${script.name} @ ${targetRps} req/s...`)
  const result = spawnSync(
    'k6',
    [
      'run',
      `--summary-export=${summaryFile}`,
      '-e',
      `SIRANNON_URL=${SIRANNON_URL}`,
      '-e',
      `POSTGRES_URL=${POSTGRES_URL}`,
      '-e',
      `TARGET_RPS=${targetRps}`,
      '-e',
      `DURATION=${DURATION}`,
      '-e',
      `DATA_SIZE=${DATA_SIZE}`,
      scriptPath,
    ],
    { stdio: 'inherit' },
  )

  if (result.status !== 0) {
    console.error(`k6 exited with code ${result.status} for ${script.name} @ ${targetRps} rps`)
    return null
  }

  return summaryFile
}

async function main() {
  if (!checkK6Installed()) {
    console.error('k6 is not installed. Install it: https://grafana.com/docs/k6/latest/set-up/install-k6/')
    process.exit(1)
  }

  mkdirSync(RESULTS_DIR, { recursive: true })
  const systemInfo = collectSystemInfo()

  console.log('Building and starting e2e containers...')
  run(`docker compose -f ${COMPOSE_FILE} --profile e2e build`)
  run(`docker compose -f ${COMPOSE_FILE} --profile e2e up -d`)

  try {
    await waitForHealth(SIRANNON_URL, 'Sirannon app')
    await waitForHealth(POSTGRES_URL, 'Postgres app')

    const allResults: Record<string, unknown>[] = []

    for (const script of scripts) {
      for (const rps of RPS_LEVELS) {
        const summaryFile = runK6(script, rps)
        if (summaryFile) {
          allResults.push({ script: script.name, targetRps: rps, summaryFile })
        }
      }
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    const summaryPath = join(RESULTS_DIR, `e2e-summary-${timestamp}.json`)
    writeFileSync(
      summaryPath,
      `${JSON.stringify(
        {
          category: 'e2e',
          timestamp: new Date().toISOString(),
          system: systemInfo,
          config: { rpsLevels: RPS_LEVELS, duration: DURATION, dataSize: DATA_SIZE },
          runs: allResults,
        },
        null,
        2,
      )}\n`,
    )

    console.log(`\nE2E benchmark summary written to ${summaryPath}`)
  } finally {
    console.log('\nStopping e2e containers...')
    run(`docker compose -f ${COMPOSE_FILE} --profile e2e down`, { ignoreError: true })
  }
}

main().catch(err => {
  console.error(err)
  run(`docker compose -f ${COMPOSE_FILE} --profile e2e down`, { ignoreError: true })
  process.exit(1)
})
