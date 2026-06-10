import { RefreshCw, RotateCcw, Shield, Signal } from 'lucide-react'
import { useCallback } from 'react'
import type { ConnectionState } from '../types'
import { Badge } from './ui/badge'
import { Button } from './ui/button'

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
          <Shield size={18} />
        </span>
        <div>
          <h1>Entitlement Control Plane</h1>
          <p>Distributed SQLite authority, replicated state, live CDC</p>
        </div>
      </div>
      <div className="topbar-status">
        <Badge variant={connectionBadge(connectionState)}>{connectionState}</Badge>
        <span className="event-ticker">
          <Signal size={14} />
          {pendingAction ?? lastEvent}
        </span>
        <Button
          variant="outline"
          size="icon"
          type="button"
          disabled={refreshing}
          onClick={handleRefreshClick}
          aria-label="Refresh"
          title="Refresh"
        >
          <RefreshCw size={16} />
        </Button>
        <Button
          variant="outline"
          size="icon"
          type="button"
          disabled={pendingAction !== null}
          onClick={handleResetClick}
          aria-label="Reset"
          title="Reset"
        >
          <RotateCcw size={16} />
        </Button>
      </div>
    </header>
  )
}

function connectionBadge(connectionState: ConnectionState): 'success' | 'warning' | 'destructive' {
  if (connectionState === 'live') {
    return 'success'
  }
  if (connectionState === 'connecting') {
    return 'warning'
  }
  return 'destructive'
}
