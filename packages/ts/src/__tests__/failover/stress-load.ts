import { setTimeout as delay } from 'node:timers/promises'
import type { GateEnvironment } from './gate-environment.js'
import { statusOf } from './gate-observations.js'

const PRIMARY_POLL_INTERVAL_MS = 200
const AUTHORITY_SAMPLE_INTERVAL_MS = 250
const RETRY_PAUSE_MS = 50
const WRITE_CONCERN_TIMEOUT_MS = 5_000
const REPLY_TIMEOUT_MS = 30_000
const INSERT_ITEM_SQL = 'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)'

export interface FailedStressWrite {
  id: number
  atMs: number
  code: string
}

export interface ChaosWindow {
  action: string
  startMs: number
  endMs: number
}

export interface DoubleAuthoritySample {
  atMs: number
  nodeIds: string[]
}

function percentile(sorted: readonly number[], fraction: number): number {
  if (sorted.length === 0) return 0
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))] ?? 0
}

function errorCode(err: unknown): string {
  if (err && typeof err === 'object' && 'code' in err && typeof err.code === 'string') return err.code
  const message = err instanceof Error ? err.message : String(err)
  if (message.includes('is not running') || message.includes('exited')) return 'NODE_STOPPED'
  if (message.startsWith('Timed out waiting for execute')) return 'REPLY_TIMEOUT'
  return 'NO_CODE'
}

export class MajorityWriteLoad {
  readonly acknowledged: number[] = []
  readonly failures: FailedStressWrite[] = []
  private readonly latenciesMs: number[] = []
  private primaryId: string | null = null
  private nextId: number
  private stopped = false
  private workers: Promise<void>[] = []
  private poller: Promise<void> = Promise.resolve()
  private startedMs = 0

  constructor(
    private readonly environment: GateEnvironment,
    private readonly concurrency: number,
    firstId: number,
  ) {
    this.nextId = firstId
  }

  get attempted(): number {
    return this.acknowledged.length + this.failures.length
  }

  start(): void {
    this.startedMs = Date.now()
    this.poller = this.pollPrimary()
    this.workers = Array.from({ length: this.concurrency }, () => this.writeUntilStopped())
  }

  async stop(): Promise<void> {
    this.stopped = true
    await Promise.all([...this.workers, this.poller])
  }

  summary(windows: readonly ChaosWindow[]): Record<string, unknown> {
    const elapsedMs = Math.max(1, Date.now() - this.startedMs)
    const sorted = [...this.latenciesMs].sort((left, right) => left - right)
    const byCode: Record<string, number> = {}
    for (const failure of this.failures) byCode[failure.code] = (byCode[failure.code] ?? 0) + 1
    const byAction: Record<string, number> = {}
    for (const failure of this.failures) {
      const window = windows.find(candidate => failure.atMs >= candidate.startMs && failure.atMs <= candidate.endMs)
      const action = window?.action ?? 'steady'
      byAction[action] = (byAction[action] ?? 0) + 1
    }
    return {
      attempted: this.attempted,
      acknowledged: this.acknowledged.length,
      failed: this.failures.length,
      failuresByCode: byCode,
      failuresByAction: byAction,
      acknowledgedPerSecond: Math.round((this.acknowledged.length * 1000) / elapsedMs),
      latencyMs: { p50: percentile(sorted, 0.5), p99: percentile(sorted, 0.99), max: sorted[sorted.length - 1] ?? 0 },
    }
  }

  private async pollPrimary(): Promise<void> {
    while (!this.stopped) {
      const group = await this.environment.etcdOracle.group().catch(() => null)
      const primary = group?.currentPrimary?.nodeId ?? null
      this.primaryId = primary && !group?.repairingNodeIds.includes(primary) ? primary : null
      await delay(PRIMARY_POLL_INTERVAL_MS)
    }
  }

  private async writeUntilStopped(): Promise<void> {
    while (!this.stopped) {
      const primaryId = this.primaryId
      const node = primaryId ? this.environment.nodes.get(primaryId) : undefined
      if (!primaryId || !node?.running) {
        await delay(RETRY_PAUSE_MS)
        continue
      }
      const id = this.nextId++
      const startedMs = performance.now()
      try {
        await node.execute(
          INSERT_ITEM_SQL,
          [id, primaryId, id, 'stress'],
          { writeConcern: { level: 'majority', timeoutMs: WRITE_CONCERN_TIMEOUT_MS } },
          REPLY_TIMEOUT_MS,
        )
        this.acknowledged.push(id)
        this.latenciesMs.push(Math.round(performance.now() - startedMs))
      } catch (err: unknown) {
        this.failures.push({ id, atMs: Date.now(), code: errorCode(err) })
        await delay(RETRY_PAUSE_MS)
      }
    }
  }
}

export class AuthoritySampler {
  readonly doubleAuthority: DoubleAuthoritySample[] = []
  samples = 0
  private stopped = false
  private loop: Promise<void> = Promise.resolve()

  constructor(private readonly environment: GateEnvironment) {}

  start(): void {
    this.loop = this.sampleUntilStopped()
  }

  async stop(): Promise<void> {
    this.stopped = true
    await this.loop
  }

  private async sampleUntilStopped(): Promise<void> {
    while (!this.stopped) {
      const nodes = [...this.environment.nodes.values()].filter(node => node.running)
      const statuses = await Promise.allSettled(nodes.map(statusOf))
      const writable: string[] = []
      for (const result of statuses) {
        if (result.status !== 'fulfilled') continue
        if (result.value.coordinator?.authority && result.value.health.canWrite) writable.push(result.value.nodeId)
      }
      this.samples += 1
      if (writable.length > 1) this.doubleAuthority.push({ atMs: Date.now(), nodeIds: writable })
      await delay(AUTHORITY_SAMPLE_INTERVAL_MS)
    }
  }
}
