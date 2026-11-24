import type { CheckpointResponse } from '../server/checkpoint-protocol.js'
import { RemoteError } from './types.js'

function isFrameCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

/**
 * Returns the checkpoint result that a server's answer holds, after checking that its counts agree with each other.
 *
 * @param answer - The body that the server sent.
 * @returns The checkpoint result.
 * @throws A `RemoteError` with `INVALID_RESPONSE` when the body holds no consistent checkpoint result.
 * @internal
 */
export function checkpointResponseFrom(answer: unknown): CheckpointResponse {
  const body = (answer ?? {}) as Partial<Record<keyof CheckpointResponse, unknown>>
  const { busy, framesInLog, framesCheckpointed, emptied } = body
  if (
    typeof busy !== 'boolean' ||
    typeof emptied !== 'boolean' ||
    !isFrameCount(framesInLog) ||
    !isFrameCount(framesCheckpointed) ||
    framesCheckpointed > framesInLog ||
    (emptied && (busy || framesInLog !== 0))
  ) {
    throw new RemoteError(
      'INVALID_RESPONSE',
      'The server answered the checkpoint with a body that holds no checkpoint result',
    )
  }
  return { busy, framesInLog, framesCheckpointed, emptied }
}
