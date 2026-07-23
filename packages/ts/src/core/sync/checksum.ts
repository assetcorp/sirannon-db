import { createHash } from 'node:crypto'
import { canonicaliseForChecksum } from './canonicalise.js'
import type { ReplicationChange } from './types.js'

export function computeChecksum(changes: ReplicationChange[]): string {
  const hash = createHash('sha256')
  hash.update(canonicaliseForChecksum(changes))
  return hash.digest('hex')
}
