import type { Config } from './config.ts'
import type { ClientCeiling } from './loadgen.ts'
import { transportOf } from './network.ts'

const SIRANNON_CLIENT = '@delali/sirannon-db SDK over its WebSocket transport'
const POSTGRES_CLIENT = 'node-postgres (pg) over a pooled binary socket'

function sirannonHost(config: Config): string {
  try {
    return new URL(config.sirannon.baseUrl).hostname.replace(/^\[|\]$/g, '')
  } catch {
    return config.sirannon.baseUrl
  }
}

function transport(config: Config): 'loopback' | 'network' {
  return transportOf(config.postgres.host) === 'network' || transportOf(sirannonHost(config)) === 'network'
    ? 'network'
    : 'loopback'
}

const LOOPBACK_NOTE =
  'Both engines and the load driver run as native processes on one host with no network between the ' +
  'load driver and the server. The engine under test is pinned to its own CPU cores under a hard ' +
  'memory ceiling (cgroup v2), and the load driver runs on disjoint pinned cores with no memory cap, ' +
  'so it never competes with the engine under test.'

const NETWORK_NOTE =
  'The load driver runs on its own machine and reaches the engine over the private network between ' +
  'the two machines, so every request crosses that network. The engine under test runs as a native ' +
  'process under a hard memory ceiling (cgroup v2) with every CPU of its machine, and it shares that ' +
  'machine with nothing but the harness.'

export function deliveryDisclosure(config: Config): Record<string, unknown> {
  const via = transport(config)
  return {
    sirannon: 'websocket',
    postgres: 'socket',
    transport: via,
    sirannon_client: SIRANNON_CLIENT,
    postgres_client: POSTGRES_CLIENT,
    driver_cpus: config.driverCpus,
    engine_cpus: config.engineCpus,
    note:
      "Each engine is driven through the client it provides. Sirannon runs over its SDK's " +
      'default WebSocket transport, which multiplexes every concurrent request over one persistent ' +
      'socket, and PostgreSQL runs over node-postgres on its binary socket protocol, with one pooled ' +
      'connection for each request that the generator allows in flight. One Node load generator ' +
      'drives both, so the coordinated-omission instrument behaves identically for each. ' +
      (via === 'network' ? NETWORK_NOTE : LOOPBACK_NOTE),
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
    concurrency_levels: config.concurrencyLevels,
    target_rates: config.targetRates,
    sweep_stop_steps: config.sweepStopSteps,
    prepare_retry_seconds: config.prepareRetrySeconds,
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
  ceilings: ClientCeiling[],
  clientBoundAny: boolean,
  indeterminateAny: boolean,
): Record<string, unknown> {
  const maxOfferedRate = config.targetRates.length > 0 ? Math.max(...config.targetRates) : 0
  const first = ceilings[0]
  return {
    client: clientName(engine),
    probe_statement: first?.statement ?? null,
    probe_seconds: first?.seconds ?? null,
    max_offered_rate: maxOfferedRate,
    levels: ceilings.map(ceiling => ({
      concurrency: ceiling.concurrency,
      ceiling_ops: ceiling.ceilingOps,
      probe_errors: ceiling.errors,
      headroom_factor: maxOfferedRate > 0 ? ceiling.ceilingOps / maxOfferedRate : 0,
    })),
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
  failedWorkloads: Record<string, unknown>[]
  features: Record<string, unknown>[]
  clientSaturation: Record<string, unknown>
  network: Record<string, unknown> | null
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
    failed_workloads: args.failedWorkloads,
    features: args.features,
    client_saturation: args.clientSaturation,
    network: args.network,
  }
}
