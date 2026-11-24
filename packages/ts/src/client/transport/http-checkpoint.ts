import type { CheckpointResponse } from '../../server/checkpoint-protocol.js'
import { postJson } from '../http-json.js'
import type { RemoteError } from '../types.js'

/**
 * Sends `POST {databaseUrl}/checkpoint` and returns the server's answer, cancelling the request when the transport
 * closes first.
 *
 * @param databaseUrl - The HTTP URL of the database, including its `/db/{id}` path.
 * @param headers - The headers to send with the request.
 * @param timeoutMs - The deadline in milliseconds, with zero disabling the deadline.
 * @param inFlight - The transport's set of open requests, which holds this request until it settles.
 * @param isClosed - Returns whether the transport has closed.
 * @param closedError - Returns the error to throw once the transport has closed.
 * @returns The answer that the server sent.
 * @internal
 */
export async function checkpointOverHttp(
  databaseUrl: string,
  headers: Record<string, string> | undefined,
  timeoutMs: number,
  inFlight: Set<AbortController>,
  isClosed: () => boolean,
  closedError: () => RemoteError,
): Promise<CheckpointResponse> {
  const request = new AbortController()
  inFlight.add(request)
  try {
    const answer = await postJson(`${databaseUrl}/checkpoint`, {}, headers, timeoutMs, request.signal)
    if (isClosed()) throw closedError()
    return answer as CheckpointResponse
  } catch (err) {
    if (isClosed()) throw closedError()
    throw err
  } finally {
    inFlight.delete(request)
  }
}
