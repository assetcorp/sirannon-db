import { setTimeout as delay } from 'node:timers/promises'
import { describe, expect, it } from 'vitest'
import { waitForIdenticalFiles } from './gate-assertions.js'
import { waitForInSyncSet } from './gate-cluster-state.js'
import { CLUSTER_ID, dbPathFor, FIVE_NODE_IDS, type GateEnvironment, GROUP_ID, NODE_IDS } from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { startCluster } from './gate-scenarios.js'
import { readRolledBackItemInsertIds } from './sqlite-oracle.js'
import {
  addLinkLatency,
  type ChaosAction,
  type ChaosContext,
  failOverFromKilledPrimary,
  freezeEtcdMember,
  PRODUCTION_TIMINGS,
  restartEveryNodeInPlace,
  restartPrimaryInPlace,
  restartReplica,
  rollingRestart,
  runChaos,
  seededRandom,
  stallReplica,
  stallTwoReplicas,
} from './stress-chaos.js'
import { AuthoritySampler, type ChaosWindow, MajorityWriteLoad } from './stress-load.js'

const WRITES_IN_FLIGHT = 256
const DEFAULT_SOAK_MS = 600_000
const STEADY_INTERVAL_MS = 10_000
const LEVEL_PHASE_MS = 20_000
const CONVERGENCE_TIMEOUT_MS = 180_000
const SETUP_ALLOWANCE_MS = 600_000
const FIRST_FEATURE_LEVEL_RECORD_FIELDS = ['featureLevel', 'inSyncMajorityTerm']

const soakMs = Number(process.env.SIRANNON_STRESS_DURATION_MS ?? DEFAULT_SOAK_MS)
const seed = Number(process.env.SIRANNON_STRESS_SEED ?? Date.now())

interface StressRun {
  context: ChaosContext
  load: MajorityWriteLoad
  sampler: AuthoritySampler
}

async function recordedFeatureLevel(environment: GateEnvironment): Promise<unknown> {
  const record = await environment.etcdOracle.groupRecord(GROUP_ID)
  return record?.featureLevel ?? 1
}

function beginLoad(environment: GateEnvironment, members: readonly string[]): StressRun {
  const windows: ChaosWindow[] = []
  const context: ChaosContext = { environment, members, windows, random: seededRandom(seed) }
  const load = new MajorityWriteLoad(environment, WRITES_IN_FLIGHT, 1)
  const sampler = new AuthoritySampler(environment)
  load.start()
  sampler.start()
  return { context, load, sampler }
}

async function cycleChaos(run: StressRun, actions: ReadonlyArray<[string, ChaosAction]>, untilMs: number) {
  for (let index = 0; Date.now() < untilMs; index++) {
    const entry = actions[index % actions.length]
    if (!entry) break
    await runChaos(run.context, entry[0], entry[1])
    await delay(STEADY_INTERVAL_MS)
  }
}

async function finishAndVerify(
  scenario: string,
  run: StressRun,
  extra: Record<string, unknown> = {},
  steadyFromMs = 0,
): Promise<void> {
  await run.load.stop()
  await run.sampler.stop()
  const { environment, members, windows } = run.context
  await waitForInSyncSet(environment, members, CONVERGENCE_TIMEOUT_MS)
  const rows = await waitForIdenticalFiles(environment, members, CONVERGENCE_TIMEOUT_MS)
  const held = new Set(rows.map(row => row.id))
  const lost = run.load.acknowledged.filter(id => !held.has(id))
  const acknowledged = new Set(run.load.acknowledged)
  const rolledBack = members.flatMap(nodeId => readRolledBackItemInsertIds(dbPathFor(environment, nodeId)))
  const acknowledgedButRolledBack = rolledBack.filter(id => acknowledged.has(id))
  const steadyFailures = run.load.failures.filter(
    failure =>
      failure.atMs >= steadyFromMs &&
      !windows.some(window => failure.atMs >= window.startMs && failure.atMs <= window.endMs),
  )
  const report = {
    scenario,
    seed,
    ...run.load.summary(windows),
    rowsOnEveryNode: rows.length,
    acknowledgedButLost: lost.length,
    authoritySamples: run.sampler.samples,
    doubleAuthoritySamples: run.sampler.doubleAuthority.length,
    rolledBackInserts: rolledBack.length,
    acknowledgedButRolledBack: acknowledgedButRolledBack.length,
    chaos: windows.map(window => ({ action: window.action, ms: window.endMs - window.startMs })),
    ...extra,
  }
  console.log(`SIRANNON_STRESS_REPORT ${JSON.stringify(report)}`)
  expect(lost).toEqual([])
  expect(acknowledgedButRolledBack).toEqual([])
  expect(run.sampler.doubleAuthority).toEqual([])
  expect(steadyFailures).toEqual([])
}

describe('a three-voter group under sustained majority writes, link latency, and repeated faults', () => {
  const environment = useFailoverEnvironment()

  it(
    'loses no acknowledged write through a rolling upgrade from feature level 1 and repeated faults at level 2',
    async () => {
      const env = environment()
      await startCluster(env, { everyNode: PRODUCTION_TIMINGS })
      const record = (await env.etcdOracle.groupRecord(GROUP_ID)) ?? {}
      const levelOneRecord = Object.fromEntries(
        Object.entries(record).filter(([field]) => !FIRST_FEATURE_LEVEL_RECORD_FIELDS.includes(field)),
      )
      await env.etcdOracle.writeGroupRecord(GROUP_ID, JSON.stringify(levelOneRecord))
      expect(await recordedFeatureLevel(env)).toBe(1)
      await addLinkLatency(env)

      const run = beginLoad(env, NODE_IDS)
      const levelOneStartVersion = await env.etcdOracle.groupRecordVersion()
      await delay(LEVEL_PHASE_MS)
      const levelOneWrites = (await env.etcdOracle.groupRecordVersion()) - levelOneStartVersion
      await runChaos(run.context, 'rolling-restart-at-level-1', rollingRestart)

      await env.coordinator.raiseFeatureLevel({ clusterId: CLUSTER_ID, groupId: GROUP_ID, featureLevel: 2 })
      await delay(STEADY_INTERVAL_MS)
      const levelTwoStartVersion = await env.etcdOracle.groupRecordVersion()
      await delay(LEVEL_PHASE_MS)
      const levelTwoWrites = (await env.etcdOracle.groupRecordVersion()) - levelTwoStartVersion

      await cycleChaos(
        run,
        [
          ['stall-replica', stallReplica],
          ['kill-primary-and-fail-over', failOverFromKilledPrimary],
          ['restart-replica', restartReplica],
          ['freeze-etcd-member', freezeEtcdMember],
          ['restart-primary-in-place', restartPrimaryInPlace],
          ['rolling-restart', rollingRestart],
        ],
        Date.now() + soakMs,
      )
      await finishAndVerify('three-voters', run, { levelOneWrites, levelTwoWrites })
      expect(levelOneWrites).toBeGreaterThan(levelTwoWrites)
      expect(await recordedFeatureLevel(env)).toBe(2)
    },
    soakMs + SETUP_ALLOWANCE_MS,
  )
})

describe('a three-voter group upgraded node by node from the published 0.4.0 release under sustained majority writes', () => {
  const environment = useFailoverEnvironment()

  it(
    'loses no acknowledged write while 0.4.0 and current nodes share the group, and then raises the feature level',
    async () => {
      const env = environment()
      await startCluster(env, { everyNode: { ...PRODUCTION_TIMINGS, publishedRelease: true } })
      expect(await recordedFeatureLevel(env)).toBe(1)
      await addLinkLatency(env)

      const run = beginLoad(env, NODE_IDS)
      await delay(LEVEL_PHASE_MS)
      const upgradeStartMs = Date.now()
      await runChaos(run.context, 'upgrade-from-0.4.0', restartEveryNodeInPlace)
      await delay(STEADY_INTERVAL_MS)
      await env.coordinator.raiseFeatureLevel({ clusterId: CLUSTER_ID, groupId: GROUP_ID, featureLevel: 2 })
      await delay(STEADY_INTERVAL_MS)
      await cycleChaos(
        run,
        [
          ['kill-primary-and-fail-over', failOverFromKilledPrimary],
          ['restart-replica', restartReplica],
        ],
        Date.now() + LEVEL_PHASE_MS,
      )
      const failuresUnderPublishedRelease = run.load.failures.filter(failure => failure.atMs < upgradeStartMs).length
      await finishAndVerify('upgrade-from-0.4.0', run, { failuresUnderPublishedRelease }, upgradeStartMs)
      expect(await recordedFeatureLevel(env)).toBe(2)
    },
    SETUP_ALLOWANCE_MS * 2,
  )
})

describe('a five-voter group under sustained majority writes, link latency, and repeated faults', () => {
  const environment = useFailoverEnvironment(FIVE_NODE_IDS)

  it(
    'loses no acknowledged write, never has two writable primaries, and fails no write outside a fault',
    async () => {
      const env = environment()
      await startCluster(env, { everyNode: PRODUCTION_TIMINGS, members: FIVE_NODE_IDS, voters: FIVE_NODE_IDS })
      await addLinkLatency(env)
      const run = beginLoad(env, FIVE_NODE_IDS)
      await cycleChaos(
        run,
        [
          ['stall-two-replicas', stallTwoReplicas],
          ['kill-primary-and-fail-over', failOverFromKilledPrimary],
          ['restart-replica', restartReplica],
          ['freeze-etcd-member', freezeEtcdMember],
          ['restart-primary-in-place', restartPrimaryInPlace],
        ],
        Date.now() + Math.round(soakMs / 2),
      )
      await finishAndVerify('five-voters', run)
    },
    soakMs + SETUP_ALLOWANCE_MS,
  )
})
