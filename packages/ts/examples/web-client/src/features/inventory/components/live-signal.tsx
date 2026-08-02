import { RadioTower } from 'lucide-react'
import type { ConnectionState } from '../types'

function headline(connectionState: ConnectionState, revalidating: boolean, pendingAction: string | null): string {
  if (pendingAction !== null) {
    return pendingAction
  }

  if (connectionState === 'offline') {
    return 'Live queries detached'
  }

  if (connectionState === 'connecting') {
    return 'Opening live queries'
  }

  return revalidating ? 'Re-reading after a wide change' : 'Live queries attached'
}

export function LiveSignal({
  connectionState,
  revalidating,
  pendingAction,
  productCount,
  activityCount,
}: {
  connectionState: ConnectionState
  revalidating: boolean
  pendingAction: string | null
  productCount: number
  activityCount: number
}) {
  return (
    <div className="live-signal">
      <RadioTower size={18} />
      <output aria-live="polite">
        <strong>{headline(connectionState, revalidating, pendingAction)}</strong>
        <span>
          {productCount} products, {activityCount} log entries
        </span>
      </output>
    </div>
  )
}
