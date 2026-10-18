export interface StoredGroup {
  votingDataBearingNodeIds: string[]
  currentPrimary: { nodeId: string; endpoint?: string } | null
  primaryTerm: bigint
  durabilityPointSeq: bigint
  inSyncNodeIds: string[]
  drainingNodeIds: string[]
  repairingNodeIds: string[]
  faultedNodeIds: string[]
  compatibility?: { packageVersion?: string; specVersion?: string; protocolVersion?: string }
}

export interface StoredNodeSession {
  nodeId: string
  leaseId: string
  etcdLeaseId: string
  createRevision: bigint
  endpoint?: string
  voting: boolean
  compatibility?: { packageVersion?: string; specVersion?: string; protocolVersion?: string }
}

export interface StoredController {
  holderId: string
  leaseId: string
  createRevision: bigint
}

interface RawKeyValue {
  key: string
  value: string
  create_revision?: string
  mod_revision?: string
  lease?: string
}

interface RangeResponse {
  kvs?: RawKeyValue[]
}

export class EtcdOracle {
  constructor(
    private readonly endpoints: readonly string[],
    private readonly keyPrefix: string,
    private readonly clusterId: string,
    private readonly groupId: string,
  ) {}

  async group(): Promise<StoredGroup | null> {
    const kv = await this.readKey(
      `clusters/${encodeURIComponent(this.clusterId)}/groups/${encodeURIComponent(this.groupId)}`,
    )
    if (!kv) return null
    const raw = parseObject(kv.value)
    return {
      votingDataBearingNodeIds: stringList(raw.votingDataBearingNodeIds),
      currentPrimary: primaryOf(raw.currentPrimary),
      primaryTerm: BigInt(String(raw.primaryTerm)),
      durabilityPointSeq: BigInt(String(raw.durabilityPointSeq ?? '0')),
      inSyncNodeIds: stringList(raw.inSyncNodeIds),
      drainingNodeIds: stringList(raw.drainingNodeIds),
      repairingNodeIds: stringList(raw.repairingNodeIds),
      faultedNodeIds: stringList(raw.faultedNodeIds ?? []),
      compatibility: compatibilityOf(raw.compatibility),
    }
  }

  async nodeSession(nodeId: string): Promise<StoredNodeSession | null> {
    const kv = await this.readKey(`clusters/${encodeURIComponent(this.clusterId)}/nodes/${encodeURIComponent(nodeId)}`)
    if (!kv) return null
    const raw = parseObject(kv.value)
    const lease = objectOf(raw.lease)
    return {
      nodeId: String(raw.nodeId),
      leaseId: String(lease.id),
      etcdLeaseId: kv.lease ?? '0',
      createRevision: BigInt(kv.create_revision ?? '0'),
      endpoint: typeof raw.endpoint === 'string' ? raw.endpoint : undefined,
      voting: raw.voting === true,
      compatibility: compatibilityOf(raw.compatibility),
    }
  }

  async liveNodeIds(): Promise<string[]> {
    const prefix = `clusters/${encodeURIComponent(this.clusterId)}/nodes/`
    const kvs = await this.readPrefix(prefix)
    return kvs.map(kv => decodeURIComponent(kv.key.slice(this.fullKey(prefix).length))).sort()
  }

  async controller(): Promise<StoredController | null> {
    const kv = await this.readKey(`clusters/${encodeURIComponent(this.clusterId)}/controller`)
    if (!kv) return null
    const raw = parseObject(kv.value)
    return {
      holderId: String(raw.holderId),
      leaseId: String(raw.id),
      createRevision: BigInt(kv.create_revision ?? '0'),
    }
  }

  async keysUnderPrefix(): Promise<string[]> {
    const kvs = await this.readPrefix('')
    return kvs.map(kv => kv.key)
  }

  async leaseGrantedTotal(): Promise<number> {
    let total = 0
    for (const endpoint of this.endpoints) {
      const response = await fetch(`${endpoint}/metrics`, { signal: AbortSignal.timeout(2_000) })
      const text = await response.text()
      const line = text.split('\n').find(entry => entry.startsWith('etcd_debugging_lease_granted_total '))
      if (!line) throw new Error(`etcd at ${endpoint} reports no lease grant counter`)
      total = Math.max(total, Number(line.split(' ')[1]))
    }
    return total
  }

  private fullKey(relativeKey: string): string {
    const trimmed = this.keyPrefix.replace(/^\/+/, '').replace(/\/+$/, '')
    return `${trimmed}/${relativeKey}`
  }

  private async readKey(
    relativeKey: string,
  ): Promise<{ key: string; value: string; lease?: string; create_revision?: string } | null> {
    const response = await this.range({ key: encode(this.fullKey(relativeKey)) })
    const kv = response.kvs?.[0]
    if (!kv) return null
    return { ...kv, key: decode(kv.key), value: decode(kv.value) }
  }

  private async readPrefix(relativePrefix: string): Promise<Array<{ key: string; value: string }>> {
    const start = this.fullKey(relativePrefix)
    const response = await this.range({ key: encode(start), range_end: encode(prefixEnd(start)) })
    return (response.kvs ?? []).map(kv => ({ key: decode(kv.key), value: decode(kv.value) }))
  }

  private async range(body: Record<string, string>): Promise<RangeResponse> {
    let lastError: unknown = null
    for (const endpoint of this.endpoints) {
      try {
        const response = await fetch(`${endpoint}/v3/kv/range`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(2_000),
        })
        if (!response.ok) {
          lastError = new Error(`etcd range at ${endpoint} returned ${response.status}: ${await response.text()}`)
          continue
        }
        return (await response.json()) as RangeResponse
      } catch (err: unknown) {
        lastError = err
      }
    }
    throw lastError instanceof Error ? lastError : new Error(String(lastError))
  }
}

function encode(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64')
}

function decode(value: string): string {
  return Buffer.from(value, 'base64').toString('utf8')
}

function prefixEnd(prefix: string): string {
  const bytes = Buffer.from(prefix, 'utf8')
  for (let index = bytes.length - 1; index >= 0; index--) {
    const byte = bytes[index]
    if (byte !== undefined && byte < 0xff) {
      bytes[index] = byte + 1
      return bytes.subarray(0, index + 1).toString('utf8')
    }
  }
  return '\0'
}

function parseObject(text: string): Record<string, unknown> {
  return objectOf(JSON.parse(text))
}

function objectOf(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`etcd holds a value that is not an object: ${JSON.stringify(value)}`)
  }
  return value as Record<string, unknown>
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) throw new Error(`etcd holds a value that is not a list: ${JSON.stringify(value)}`)
  return value.map(entry => String(entry))
}

function primaryOf(value: unknown): StoredGroup['currentPrimary'] {
  if (value === null || value === undefined) return null
  const raw = objectOf(value)
  return typeof raw.endpoint === 'string'
    ? { nodeId: String(raw.nodeId), endpoint: raw.endpoint }
    : { nodeId: String(raw.nodeId) }
}

function compatibilityOf(value: unknown): StoredGroup['compatibility'] {
  if (value === null || value === undefined) return undefined
  const raw = objectOf(value)
  return {
    packageVersion: typeof raw.packageVersion === 'string' ? raw.packageVersion : undefined,
    specVersion: typeof raw.specVersion === 'string' ? raw.specVersion : undefined,
    protocolVersion: typeof raw.protocolVersion === 'string' ? raw.protocolVersion : undefined,
  }
}
