// Assemble the per-engine result file and its disclosure block. One file records one engine at one
// durability level: the machine, the engine version and durability settings in force, the full
// configuration so a reader can reproduce the run, the per-workload numbers, and the load client's
// own measured throughput ceiling. The aggregate step reads these files to build the cross-engine
// comparison.

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
      'Each engine is driven through the client it actually ships. Sirannon runs over its SDK\'s ' +
      'default WebSocket transport, which multiplexes every concurrent request over one persistent ' +
      'socket, and PostgreSQL runs over node-postgres on its binary socket protocol. One Node load ' +
      'generator drives both, so the coordinated-omission instrument behaves identically for each. ' +
      'Both run on one host with no network between the load driver and the server, and the load ' +
      'driver runs on its own pinned cores so it never competes with the engine under test.',
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
): Record<string, unknown> {
  const maxOfferedRate = config.targetRates.length > 0 ? Math.max(...config.targetRates) : 0
  const headroomFactor = maxOfferedRate > 0 ? ceiling.ceilingOps / maxOfferedRate : 0
  return {
    client: clientName(engine),
    ceiling_ops: ceiling.ceilingOps,
    probe_concurrency: ceiling.concurrency,
    probe_seconds: ceiling.seconds,
    probe_errors: ceiling.errors,
    max_offered_rate: maxOfferedRate,
    headroom_factor: headroomFactor,
    client_bound_any: clientBoundAny,
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
