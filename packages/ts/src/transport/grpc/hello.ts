import type { NodeInfo } from '../../replication/types.js'
import type { Hello } from './generated/replication.js'
import type { GrpcReplicationTransport } from './transport.js'

/**
 * Returns the `Hello` that this node sends first on every stream, with `encrypted` false where the engine supplied
 * nothing.
 *
 * @param t - The transport whose node the `Hello` describes.
 * @returns The `Hello` message.
 * @internal
 */
export function localHello(t: GrpcReplicationTransport): Hello {
  return {
    nodeId: t.localNodeId,
    role: t.localRole,
    groupId: t.localGroupId ?? '',
    primaryTerm: t.localPrimaryTerm ?? 0n,
    protocolVersion: t.localProtocolVersion ?? '',
    encrypted: t.localEncrypted,
  }
}

/**
 * Returns what a peer's `Hello` states about the peer, where a peer that sends no `encrypted` field counts as
 * plaintext.
 *
 * @param hello - The peer's `Hello`.
 * @returns The fields that the transport records for the peer.
 * @internal
 */
export function peerHelloFields(
  hello: Hello,
): Pick<NodeInfo, 'groupId' | 'primaryTerm' | 'protocolVersion' | 'encrypted'> {
  return {
    groupId: hello.groupId || undefined,
    primaryTerm: hello.primaryTerm === 0n ? undefined : hello.primaryTerm,
    protocolVersion: hello.protocolVersion || undefined,
    encrypted: hello.encrypted === true,
  }
}
