import { Activity, RefreshCw, RotateCcw, ShieldCheck } from 'lucide-react'
import { useCallback } from 'react'
import { Button } from '@/components/ui/button'
import { Separator } from '@/components/ui/separator'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import type { ConnectionState } from '../types'
import { StatusDot, type StatusTone } from './status'

const CONNECTION_TONE: Record<ConnectionState, StatusTone> = {
  live: 'success',
  connecting: 'warning',
  offline: 'destructive',
}

const CONNECTION_TEXT: Record<ConnectionState, string> = {
  live: 'text-success',
  connecting: 'text-warning',
  offline: 'text-destructive',
}

export function AppHeader({
  connectionState,
  refreshing,
  pendingAction,
  writeAvailable,
  writeUnavailableReason,
  lastEvent,
  onRefresh,
  onReset,
}: {
  connectionState: ConnectionState
  refreshing: boolean
  pendingAction: string | null
  writeAvailable: boolean
  writeUnavailableReason: string
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
    <header className="border-border/70 bg-background/70 sticky top-0 z-40 border-b backdrop-blur-md">
      <div className="mx-auto flex h-14 w-full max-w-[1440px] items-center justify-between gap-4 px-4 lg:px-6">
        <div className="flex min-w-0 items-center gap-3">
          <span className="border-border bg-card flex size-8 shrink-0 items-center justify-center rounded-lg border">
            <ShieldCheck className="size-4" aria-hidden="true" />
          </span>
          <div className="flex min-w-0 flex-col">
            <h1 className="truncate text-sm leading-tight font-semibold tracking-tight">Sirannon Entitlements</h1>
            <p className="text-muted-foreground truncate text-xs leading-tight">
              Distributed control plane with live change capture
            </p>
          </div>
        </div>

        <div className="flex shrink-0 items-center gap-3">
          <span className="text-muted-foreground hidden max-w-72 items-center gap-2 lg:flex">
            <Activity className="size-3.5 shrink-0" aria-hidden="true" />
            <span className="truncate font-mono text-xs">{pendingAction ?? lastEvent}</span>
          </span>
          <Separator orientation="vertical" className="hidden h-5 lg:block" />
          <span className="flex items-center gap-2" aria-live="polite">
            <StatusDot tone={CONNECTION_TONE[connectionState]} pulse={connectionState === 'connecting'} />
            <span className={`font-mono text-xs tracking-wider uppercase ${CONNECTION_TEXT[connectionState]}`}>
              {connectionState}
            </span>
          </span>
          <div className="flex items-center gap-1.5">
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon"
                  type="button"
                  disabled={refreshing}
                  onClick={handleRefreshClick}
                  aria-label="Refresh snapshot"
                >
                  <RefreshCw
                    className={refreshing ? 'animate-spin motion-reduce:animate-none' : undefined}
                    aria-hidden="true"
                  />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Refresh snapshot</TooltipContent>
            </Tooltip>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon"
                  type="button"
                  disabled={pendingAction !== null || !writeAvailable}
                  onClick={handleResetClick}
                  aria-label="Reset control plane"
                >
                  <RotateCcw aria-hidden="true" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                {writeAvailable ? 'Reset and reseed the control plane' : writeUnavailableReason}
              </TooltipContent>
            </Tooltip>
          </div>
        </div>
      </div>
    </header>
  )
}
