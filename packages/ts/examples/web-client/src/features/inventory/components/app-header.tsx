import { Database, RefreshCw, RotateCcw } from 'lucide-react'
import type { ConnectionState } from '../types'
import { IconButton } from './icon-button'
import { StatusPill } from './status-pill'

export function AppHeader({
  connectionState,
  refreshing,
  pendingAction,
  onRefresh,
  onReset,
}: {
  connectionState: ConnectionState
  refreshing: boolean
  pendingAction: string | null
  onRefresh: () => void
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
          label="Refresh snapshot"
          title="Refresh snapshot"
          disabled={refreshing || busy}
          onClick={onRefresh}
          icon={<RefreshCw size={16} />}
        />
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
