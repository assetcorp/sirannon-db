import { randomBytes } from 'node:crypto'
import { ReplicationError } from './errors.js'

const NODE_ID_RE = /^[0-9a-f]{32}$/

/** Generate a cryptographically random 32-hex-character node identifier. */
export function generateNodeId(): string {
  return randomBytes(16).toString('hex')
}

/** Throw a ReplicationError if the given string is not a valid 32-hex-character node ID. */
export function validateNodeId(id: string): void {
  if (!NODE_ID_RE.test(id)) {
    throw new ReplicationError(`Invalid node ID '${id}': must be 32 lowercase hex characters`)
  }
}
