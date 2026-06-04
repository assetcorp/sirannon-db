import { RefreshCw, RotateCcw, Shield, Signal } from 'lucide-react'
import { useCallback } from 'react'
import type { ConnectionState } from '../types'

export function AppHeader({
  connectionState,
  refreshing,
  pendingAction,
  lastEvent,
  onRefresh,
  onReset,
}: {
  connectionState: ConnectionState
  refreshing: boolean
  pendingAction: string | null
  lastEvent: string
  onRefresh: () => void
  onReset: () => Promise<void>
}) {
  const handleRefreshClick = useCallback(() => {
    onRefresh()
  }, [onRefresh])

  const handleResetClick = useCallback(() => {
    void onReset()
  }, [onReset])

  return (
    <header className="topbar">
      <div className="brand-block">
        <span className="brand-mark">
          <Shield size={19} />
        </span>
        <div>
          <h1>Sirannon Distributed Entitlements</h1>
          <p>Three durable SQLite nodes, coordinator failover, majority writes, live subscriptions</p>
        </div>
      </div>
      <div className="topbar-status">
        <span className={`status-pill ${connectionState}`}>{connectionState}</span>
        <span className="event-ticker">
          <Signal size={14} />
          {pendingAction ?? lastEvent}
        </span>
        <button
          className="icon-button"
          type="button"
          disabled={refreshing}
          onClick={handleRefreshClick}
          title="Refresh"
        >
          <RefreshCw size={17} />
        </button>
        <button
          className="icon-button"
          type="button"
          disabled={pendingAction !== null}
          onClick={handleResetClick}
          title="Reset"
        >
          <RotateCcw size={17} />
        </button>
      </div>
    </header>
  )
}
