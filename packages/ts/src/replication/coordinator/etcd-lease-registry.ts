import type { Lease } from 'etcd3'
import { CoordinatorError } from '../errors.js'
import type { SerializedNodeSession } from './etcd-codec.js'

export interface LocalLeaseEntry {
  lease: Lease
  leaseId: string
  key: string
  ttlMs: number
  ttlSeconds: number
  kind: 'controller' | 'node-session'
  clusterId: string
  holderId: string
  metadata?: Record<string, unknown>
  nodeSession?: Omit<SerializedNodeSession, 'lease'>
}

export async function revokeLeaseQuietly(lease: Lease): Promise<void> {
  try {
    await lease.revoke()
  } catch {
    lease.release()
  }
}

export class EtcdLeaseRegistry {
  private readonly entries = new Map<string, LocalLeaseEntry>()

  constructor(private readonly onLeaseLost: ((error: Error) => void) | undefined) {}

  get(leaseId: string): LocalLeaseEntry | undefined {
    return this.entries.get(leaseId)
  }

  forget(leaseId: string): void {
    this.entries.delete(leaseId)
  }

  leaseIdsForKey(key: string): string[] {
    const matches: string[] = []
    for (const [leaseId, entry] of this.entries) {
      if (entry.key === key) matches.push(leaseId)
    }
    return matches
  }

  track(lease: Lease, entry: Omit<LocalLeaseEntry, 'lease'>): void {
    this.entries.set(entry.leaseId, { ...entry, lease })
    lease.on('lost', err => {
      this.entries.delete(entry.leaseId)
      this.onLeaseLost?.(err instanceof Error ? err : new CoordinatorError(String(err)))
    })
  }

  async discardSuperseded(key: string, keepLeaseId: string): Promise<void> {
    const superseded: LocalLeaseEntry[] = []
    for (const [leaseId, entry] of this.entries) {
      if (entry.key === key && leaseId !== keepLeaseId) {
        this.entries.delete(leaseId)
        superseded.push(entry)
      }
    }
    await Promise.allSettled(superseded.map(entry => revokeLeaseQuietly(entry.lease)))
  }

  async revokeAll(): Promise<void> {
    const revokes: Promise<void>[] = []
    for (const entry of this.entries.values()) {
      revokes.push(revokeLeaseQuietly(entry.lease))
    }
    this.entries.clear()
    await Promise.allSettled(revokes)
  }
}
