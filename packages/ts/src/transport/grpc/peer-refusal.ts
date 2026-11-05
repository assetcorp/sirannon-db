import { Metadata, type ServiceError, status } from '@grpc/grpc-js'
import type { SirannonError } from '../../core/errors.js'
import type { GrpcReplicationTransport } from './transport.js'

/**
 * The trailer that carries the Sirannon error code of a stream that a node ends with `FAILED_PRECONDITION`.
 *
 * @internal
 */
export const ERROR_CODE_TRAILER = 'sirannon-error-code'

function refusalStatus(error: SirannonError): Error {
  const trailer = new Metadata()
  trailer.set(ERROR_CODE_TRAILER, error.code)
  return Object.assign(new Error(error.message), {
    code: status.FAILED_PRECONDITION,
    details: error.message,
    metadata: trailer,
  })
}

/**
 * Ends every `Replicate` and `Sync` stream with one peer. A stream that the peer opened ends with
 * `FAILED_PRECONDITION` and the error's code in the `sirannon-error-code` trailer, and a stream that this node opened
 * ends with a cancel, which is the only end that a gRPC client can send.
 *
 * @param t - The transport that holds the streams.
 * @param peerId - The peer to refuse.
 * @param error - The error whose code the streams carry.
 * @internal
 */
export function refusePeerStreams(t: GrpcReplicationTransport, peerId: string, error: SirannonError): void {
  const served = t.serverPeerStreams.get(peerId)
  for (const stream of [served?.replicateStream, served?.syncStream]) {
    if (stream?.writable === true) stream.emit('error', refusalStatus(error))
  }
  const dialled = t.clientPeerStreams.get(peerId)
  for (const stream of [dialled?.replicateStream, dialled?.syncStream]) {
    try {
      stream?.cancel()
    } catch {}
  }
}

/**
 * Returns the Sirannon error code of a stream that the peer ended with `FAILED_PRECONDITION` and a
 * `sirannon-error-code` trailer.
 *
 * @param err - The error that the stream emitted.
 * @returns The code, or `null` for any other end of the stream.
 * @internal
 */
export function refusalCodeOf(err: unknown): string | null {
  const serviceError = err as Partial<ServiceError> | null
  if (serviceError?.code !== status.FAILED_PRECONDITION) return null
  const [code] = serviceError.metadata?.get(ERROR_CODE_TRAILER) ?? []
  return typeof code === 'string' && code.length > 0 ? code : null
}
