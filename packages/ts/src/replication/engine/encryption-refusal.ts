import { EncryptionError } from '../../core/encryption/errors.js'
import type { NodeInfo } from '../types.js'
import type { ReplicationEngine } from './engine.js'

const PLAINTEXT_PEER_MESSAGE =
  'This node holds an encrypted database, so it sends no batch and no snapshot to a peer that announces a plaintext database'

const refusedPeers = new WeakMap<ReplicationEngine, Set<string>>()

function refusedBy(engine: ReplicationEngine): Set<string> {
  const existing = refusedPeers.get(engine)
  if (existing) return existing
  const created = new Set<string>()
  refusedPeers.set(engine, created)
  return created
}

/**
 * Returns whether this node withholds every batch and snapshot from a peer, which holds when this node's database is
 * encrypted and the peer announces a plaintext one or announces nothing.
 *
 * @param engine - This node's engine.
 * @param peer - The peer as the transport records it, or `undefined` for a peer that the transport does not hold.
 * @returns `true` when this node sends the peer no data.
 * @internal
 */
export function withholdsDataFrom(engine: ReplicationEngine, peer: NodeInfo | undefined): boolean {
  return engine.database.encryptedFile && peer?.encrypted !== true
}

/**
 * Returns the error that an encrypted node sends a plaintext peer.
 *
 * @param peerId - The plaintext peer, which the message states.
 * @returns An `EncryptionError` with `ENCRYPTION_REQUIRED`.
 * @internal
 */
export function encryptionRequired(peerId: string): EncryptionError {
  return new EncryptionError(`${PLAINTEXT_PEER_MESSAGE}, and peer '${peerId}' announces one`, 'ENCRYPTION_REQUIRED')
}

/**
 * Ends the streams of a plaintext peer through the transport where it can, and reports the refusal once for each
 * connection of that peer.
 *
 * @param engine - This node's engine.
 * @param peerId - The plaintext peer.
 * @internal
 */
export function refusePlaintextPeer(engine: ReplicationEngine, peerId: string): void {
  const refused = refusedBy(engine)
  const error = encryptionRequired(peerId)
  const transport = engine.config.transport
  if (!refused.has(peerId)) {
    refused.add(peerId)
    engine.emitError({ error, operation: 'encryption-required', peerId, recoverable: true })
  }
  transport.refusePeer?.(peerId, error).catch((err: unknown) => {
    const wrappedErr = err instanceof Error ? err : new Error(String(err))
    engine.emitError({ error: wrappedErr, operation: 'encryption-required', peerId, recoverable: true })
  })
}

/**
 * Forgets the refusal of a peer whose connection ends, so that its next connection reports the refusal again.
 *
 * @param engine - This node's engine.
 * @param peerId - The peer whose connection ended.
 * @internal
 */
export function forgetPeerRefusal(engine: ReplicationEngine, peerId: string): void {
  refusedPeers.get(engine)?.delete(peerId)
}
