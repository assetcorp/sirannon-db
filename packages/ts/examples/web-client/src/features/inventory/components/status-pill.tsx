import { statusLabel } from '../inventory-utils'
import type { ConnectionState } from '../types'

export function StatusPill({ state }: { state: ConnectionState }) {
  return <span className={`status-pill ${state}`}>{statusLabel(state)}</span>
}
