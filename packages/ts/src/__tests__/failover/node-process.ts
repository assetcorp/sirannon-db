import { type ChildProcess, spawn } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { QueryOptions } from '../../core/types.js'
import type { ForwardedTransaction, ReplicationBatch, SyncBatch, SyncRequest } from '../../replication/types.js'

export type FailoverNodeRole = 'primary' | 'replica'

export type EtcdAccess = 'insecure-test' | 'plain-http' | 'https-without-credentials' | 'https-with-ca-only'

export interface FailoverNodeConfig {
  nodeId: string
  stableNodeId: boolean
  dbPath: string
  grpcPort: number
  httpPort: number
  certPath: string
  keyPath: string
  caCertPath: string
  initialRole: FailoverNodeRole
  endpoints: string[]
  httpEndpoints: Record<string, string>
  etcdHosts: string[]
  etcdAccess: EtcdAccess
  keyPrefix: string
  clusterId: string
  groupId: string
  votingDataBearingNodeIds?: string[]
  seedSchema: boolean
  sessionTtlMs: number
  controllerLeaseTtlMs: number
  controllerTickIntervalMs: number
  controllerEnabled: boolean
  writeForwarding: boolean
  backupDirectory?: string
  compatibility: {
    packageVersion: string
    specVersion: string
    protocolVersion: string
  }
}

export interface FailoverNodeEnvironment {
  clockOffsetMs?: number
  etcd3Unresolvable?: boolean
}

export interface SerializedError {
  name: string
  message: string
  code?: string
  details?: Record<string, unknown>
}

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue }

interface ReadyMessage {
  type: 'ready'
  nodeId: string
}

interface StartupFailedMessage {
  type: 'startup-failed'
  error: SerializedError
}

interface ResponseMessage {
  type: 'response'
  id: number
  ok: boolean
  result?: JsonValue
  error?: SerializedError
}

type ChildMessage = ReadyMessage | StartupFailedMessage | ResponseMessage

interface PendingRequest {
  command: string
  payload: Record<string, unknown>
  resolve: (value: JsonValue) => void
  reject: (error: Error) => void
  timer: ReturnType<typeof setTimeout>
}

export class FailoverNodeProcess {
  private readonly child: ChildProcess
  private readonly pending = new Map<number, PendingRequest>()
  private requestId = 0
  private readonly readyPromise: Promise<void>
  private readyResolve: (() => void) | null = null
  private readyReject: ((error: Error) => void) | null = null
  private readonly exitPromise: Promise<void>
  private stdout = ''
  private stderr = ''
  private startupError: SerializedError | null = null

  constructor(
    readonly config: FailoverNodeConfig,
    environment: FailoverNodeEnvironment = {},
  ) {
    const here = dirname(fileURLToPath(import.meta.url))
    this.readyPromise = new Promise((resolve, reject) => {
      this.readyResolve = resolve
      this.readyReject = reject
    })
    this.readyPromise.catch(() => undefined)
    const imports = ['--import', 'tsx', '--import', join(here, 'shifted-clock.ts')]
    if (environment.etcd3Unresolvable) {
      imports.push('--import', join(here, 'etcd3-unresolvable.ts'))
    }
    this.child = spawn(process.execPath, [...imports, join(here, 'cluster-node.ts')], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        SIRANNON_FAILOVER_NODE_CONFIG: JSON.stringify(config),
        SIRANNON_FAILOVER_CLOCK_OFFSET_MS: String(environment.clockOffsetMs ?? 0),
      },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    })
    this.exitPromise = new Promise(resolve => {
      this.child.once('exit', () => resolve())
    })
    this.child.stdout?.on('data', chunk => {
      this.stdout = appendBounded(this.stdout, chunk.toString('utf8'))
    })
    this.child.stderr?.on('data', chunk => {
      this.stderr = appendBounded(this.stderr, chunk.toString('utf8'))
    })
    this.child.on('message', message => {
      this.handleMessage(message as ChildMessage)
    })
    this.child.on('error', err => {
      this.readyReject?.(err)
      this.rejectPendingRequests(err)
    })
    this.child.on('disconnect', () => {
      const err = new Error(`failover node ${config.nodeId} IPC channel disconnected\n${this.logs()}`)
      this.readyReject?.(err)
      this.rejectPendingRequests(err)
    })
    this.child.on('exit', (code, signal) => {
      const err = new Error(
        `failover node ${config.nodeId} exited code=${code ?? 'null'} signal=${signal ?? 'null'}\n${this.logs()}`,
      )
      this.readyReject?.(err)
      this.rejectPendingRequests(err)
    })
  }

  async ready(): Promise<void> {
    await this.readyPromise
  }

  async startupFailure(): Promise<SerializedError> {
    await this.exitPromise
    if (!this.startupError) {
      throw new Error(`failover node ${this.config.nodeId} exited without reporting a startup failure\n${this.logs()}`)
    }
    return this.startupError
  }

  get running(): boolean {
    return this.child.exitCode === null && this.child.signalCode === null
  }

  async execute(sql: string, params?: unknown[], options?: QueryOptions): Promise<JsonValue> {
    return this.request('execute', { sql, params, options }, 20_000)
  }

  async localWriteProbe(id: number, note: string): Promise<JsonValue> {
    return this.request('localWriteProbe', { id, note }, 20_000)
  }

  async query(sql: string, params?: unknown[], options?: QueryOptions): Promise<JsonValue> {
    return this.request('query', { sql, params, options }, 20_000)
  }

  async status(): Promise<JsonValue> {
    return this.request('status', {}, 10_000)
  }

  async openLiveItems(): Promise<void> {
    await this.request('openLiveItems', {}, 10_000)
  }

  async liveItemIds(): Promise<number[] | null> {
    return (await this.request('liveItemIds', {}, 10_000)) as number[] | null
  }

  async reconnectTransport(): Promise<void> {
    await this.request('reconnectTransport', {}, 20_000)
  }

  async sendRawBatch(peerId: string, batch: ReplicationBatch): Promise<void> {
    await this.request('sendRawBatch', { peerId, batch: serializeJson(batch) }, 20_000)
  }

  async requestRawSync(peerId: string, request: SyncRequest): Promise<void> {
    await this.request('requestRawSync', { peerId, request: serializeJson(request) }, 20_000)
  }

  async sendRawSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    await this.request('sendRawSyncBatch', { peerId, batch: serializeJson(batch) }, 20_000)
  }

  async sendRawForward(peerId: string, request: ForwardedTransaction): Promise<void> {
    await this.request('sendRawForward', { peerId, request: serializeJson(request) }, 20_000)
  }

  async shutdown(): Promise<void> {
    if (!this.running) return
    await this.request('shutdown', {}, 20_000).catch(() => undefined)
    await this.exited(10_000).catch(() => this.kill())
    await this.exitPromise
  }

  kill(): void {
    if (!this.running) return
    this.child.kill('SIGKILL')
  }

  async killAndWait(): Promise<void> {
    this.kill()
    await this.exitPromise
  }

  async exited(timeoutMs: number): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`${this.config.nodeId} did not exit within ${timeoutMs}ms`)), timeoutMs)
    })
    try {
      await Promise.race([this.exitPromise, timeout])
    } finally {
      clearTimeout(timer)
    }
  }

  logs(): string {
    return [`stdout:\n${this.stdout}`, `stderr:\n${this.stderr}`].join('\n')
  }

  private request(command: string, payload: Record<string, unknown>, timeoutMs: number): Promise<JsonValue> {
    if (!this.child.send || !this.running) {
      return Promise.reject(new Error(`failover node ${this.config.nodeId} is not running\n${this.logs()}`))
    }

    const id = ++this.requestId
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error(`Timed out waiting for ${command} on ${this.config.nodeId}\n${this.logs()}`))
      }, timeoutMs)
      timer.unref?.()
      this.pending.set(id, { command, payload, resolve, reject, timer })
      try {
        this.child.send({ id, command, payload }, (sendErr: Error | null) => {
          if (!sendErr) return
          const pending = this.pending.get(id)
          if (!pending) return
          clearTimeout(pending.timer)
          this.pending.delete(id)
          pending.reject(withRequestContext(sendErr, this.config.nodeId, command, payload))
        })
      } catch (err: unknown) {
        const pending = this.pending.get(id)
        if (!pending) return
        clearTimeout(pending.timer)
        this.pending.delete(id)
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        pending.reject(withRequestContext(wrappedErr, this.config.nodeId, command, payload))
      }
    })
  }

  private rejectPendingRequests(err: Error): void {
    const pendingRequests = [...this.pending.values()]
    this.pending.clear()
    for (const pending of pendingRequests) {
      clearTimeout(pending.timer)
      pending.reject(err)
    }
  }

  private handleMessage(message: ChildMessage): void {
    if (message.type === 'ready') {
      this.readyResolve?.()
      return
    }
    if (message.type === 'startup-failed') {
      this.startupError = message.error
      this.readyReject?.(deserializeError(message.error))
      return
    }
    if (message.type !== 'response') return

    const pending = this.pending.get(message.id)
    if (!pending) return
    clearTimeout(pending.timer)
    this.pending.delete(message.id)

    if (message.ok) {
      pending.resolve(message.result ?? null)
      return
    }
    pending.reject(
      withRequestContext(deserializeError(message.error), this.config.nodeId, pending.command, pending.payload),
    )
  }
}

export function serializeJson(value: unknown): JsonValue {
  return JSON.parse(
    JSON.stringify(value, (_key, nested) => {
      if (typeof nested === 'bigint') return nested.toString()
      return nested
    }),
  ) as JsonValue
}

export function deserializeError(error: SerializedError | undefined): Error {
  const result = new Error(error?.message ?? 'Unknown child-process error') as Error & {
    code?: string
    details?: Record<string, unknown>
  }
  result.name = error?.name ?? 'Error'
  result.code = error?.code
  result.details = error?.details
  return result
}

function withRequestContext(err: Error, nodeId: string, command: string, payload: Record<string, unknown>): Error {
  const withCode = err as Error & {
    code?: string
    details?: Record<string, unknown>
  }
  const contextual = new Error(
    `failover node ${nodeId} command '${command}' failed: ${err.message}\npayload=${JSON.stringify(
      serializeJson(payload),
    )}`,
  ) as Error & {
    code?: string
    details?: Record<string, unknown>
  }
  contextual.name = err.name
  contextual.code = withCode.code
  contextual.details = withCode.details
  return contextual
}

function appendBounded(current: string, next: string): string {
  const combined = current + next
  return combined.length <= 32_000 ? combined : combined.slice(combined.length - 32_000)
}
