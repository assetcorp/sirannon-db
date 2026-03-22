import { randomBytes } from 'node:crypto'
import { ReplicationError } from './errors.js'

const NODE_ID_RE = /^[0-9a-f]{32}$/

export function generateNodeId(): string {
  return randomBytes(16).toString('hex')
}

export function validateNodeId(id: string): void {
  if (!NODE_ID_RE.test(id)) {
    throw new ReplicationError(`Invalid node ID '${id}': must be 32 lowercase hex characters`)
  }
}
