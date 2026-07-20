import type { Config } from './config.ts'
import type { ClientCeiling } from './loadgen.ts'

const SIRANNON_CLIENT = '@delali/sirannon-db SDK over its WebSocket transport'
const POSTGRES_CLIENT = 'node-postgres (pg) over a pooled binary socket'

export function deliveryDisclosure(config: Config): Record<string, unknown> {
  return {
    sirannon: 'websocket',
    postgres: 'socket',
    transport: 'loopback',
    sirannon_client: SIRANNON_CLIENT,
    postgres_client: POSTGRES_CLIENT,
    driver_cpus: config.driverCpus,
    engine_cpus: config.engineCpus,
    note:
      "Each engine is driven through the client it provides. Sirannon runs over its SDK's " +
      'default WebSocket transport, which multiplexes every concurrent request over one persistent ' +
      'socket, and PostgreSQL runs over node-postgres on its binary socket protocol. One Node load ' +
      'generator drives both, so the coordinated-omission instrument behaves identically for each. ' +
      'Both run as native processes on one host with no network between the load driver and the ' +
      'server. The engine under test is pinned to its own CPU cores under a hard memory ceiling ' +
      '(cgroup v2), and the load driver runs on disjoint pinned cores with no memory cap, so it ' +
      'never competes with the engine under test.',
  }
}

export function configBlock(config: Config): Record<string, unknown> {
  return {
    data_size: config.dataSize,
    warmup_seconds: config.warmupSeconds,
    measure_seconds: config.measureSeconds,
    runs: config.runs,
    seed: config.seed,
    slo_p99_ms: config.sloP99Ms,
    max_in_flight: config.maxInFlight,
    target_rates: config.targetRates,
    sweep_stop_steps: config.sweepStopSteps,
    soak_seconds: config.soakSeconds,
    soak_workloads: config.soakWorkloads,
    workloads: config.workloads,
    scaling_workloads: config.scalingWorkloads,
    delivery: deliveryDisclosure(config),
  }
}

function clientName(engine: string): string {
  return engine === 'sirannon' ? SIRANNON_CLIENT : POSTGRES_CLIENT
}

export function clientSaturationBlock(
  engine: string,
  config: Config,
  ceiling: ClientCeiling,
  clientBoundAny: boolean,
  indeterminateAny: boolean,
): Record<string, unknown> {
  const maxOfferedRate = config.targetRates.length > 0 ? Math.max(...config.targetRates) : 0
  const headroomFactor = maxOfferedRate > 0 ? ceiling.ceilingOps / maxOfferedRate : 0
  return {
    client: clientName(engine),
    ceiling_ops: ceiling.ceilingOps,
    probe_statement: ceiling.statement,
    probe_concurrency: ceiling.concurrency,
    probe_seconds: ceiling.seconds,
    probe_errors: ceiling.errors,
    max_offered_rate: maxOfferedRate,
    headroom_factor: headroomFactor,
    client_bound_any: clientBoundAny,
    indeterminate_any: indeterminateAny,
    interpretation:
      'This ceiling is measured on the probe statement above, whose response is a few bytes. A ' +
      'workload returning real rows costs the client more per operation, so its true ceiling is ' +
      'lower than this figure by an amount the probe cannot measure. Read it as an upper bound on ' +
      "the client only: a workload reaching this rate is at the client's limit, while a workload " +
      'falling below it proves nothing on its own. Whether the server saturated is decided per rate ' +
      "from the generator's in-flight occupancy, not from this number, and headroom_factor is " +
      'therefore an optimistic bound rather than proof of headroom. Occupancy in turn shows that the ' +
      'generator was waiting on replies rather than failing to send them; separating a server too ' +
      'busy to answer from a client too slow to read the answers needs the same offered load split ' +
      'across several generator processes, which this harness does not run, so a server_saturated ' +
      'verdict rests on occupancy rather than on that stronger test.',
  }
}

export function buildEngineReport(args: {
  environment: Record<string, unknown>
  engine: string
  delivery: string
  durability: string
  engineInfo: Record<string, unknown>
  config: Config
  workloads: Record<string, unknown>[]
  features: Record<string, unknown>[]
  clientSaturation: Record<string, unknown>
}): Record<string, unknown> {
  return {
    environment: args.environment,
    engine: {
      name: args.engine,
      delivery: args.delivery,
      durability: args.durability,
      version: args.engineInfo.version ?? 'unknown',
      settings: args.engineInfo,
    },
    config: configBlock(args.config),
    workloads: args.workloads,
    features: args.features,
    client_saturation: args.clientSaturation,
  }
}
