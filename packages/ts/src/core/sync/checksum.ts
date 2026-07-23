import { canonicaliseForChecksum } from './canonicalise.js'
import { sha256Hex } from './sha256.js'
import type { ReplicationChange } from './types.js'

export function computeChecksum(changes: ReplicationChange[]): string {
  return sha256Hex(canonicaliseForChecksum(changes))
}
