import { randomBytes } from 'node:crypto'
import { ReplicationError } from './errors.js'

const NODE_ID_RE = /^[0-9a-f]{32}$/

/** Returns a new node ID of 32 lowercase hex characters, drawn from a cryptographically secure random source.
 * @public
 */
export function generateNodeId(): string {
  return randomBytes(16).toString('hex')
}

/** Returns when `id` is exactly 32 lowercase hex characters, and throws a {@link ReplicationError} for any other string.
 * @public
 */
export function validateNodeId(id: string): void {
  if (!NODE_ID_RE.test(id)) {
    throw new ReplicationError(`Invalid node ID '${id}': must be 32 lowercase hex characters`)
  }
}
