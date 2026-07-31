import { Database, RotateCcw } from 'lucide-react'
import type { ConnectionState } from '../types'
import { IconButton } from './icon-button'
import { StatusPill } from './status-pill'

export function AppHeader({
  connectionState,
  pendingAction,
  onReset,
}: {
  connectionState: ConnectionState
  pendingAction: string | null
  onReset: () => void
}) {
  const busy = pendingAction !== null

  return (
    <header className="topbar">
      <div className="brand-block">
        <span className="brand-mark">
          <Database size={20} strokeWidth={2.2} />
        </span>
        <div>
          <h1>Fulfillment Operations</h1>
          <p>Live inventory state backed by networked SQLite</p>
        </div>
      </div>
      <div className="topbar-actions">
        <StatusPill state={connectionState} />
        <IconButton
          label="Reset database"
          title="Reset database"
          disabled={busy}
          onClick={onReset}
          icon={<RotateCcw size={16} />}
        />
      </div>
    </header>
  )
}
