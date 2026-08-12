import type { SyncStatus } from '@delali/sirannon-db/client'
import { cn } from '@delali/sirannon-example-shared/lib/utils'

const STATE_PRESENTATION = {
  starting: { label: 'Connecting', dot: 'bg-warning', pulse: true },
  running: { label: 'Syncing live', dot: 'bg-success', pulse: true },
  snapshotting: { label: 'Copying snapshot', dot: 'bg-warning', pulse: true },
  paused: { label: 'Paused', dot: 'bg-warning', pulse: false },
  stopped: { label: 'Offline, working locally', dot: 'bg-destructive', pulse: false },
} as const

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline gap-1.5">
      <span className="text-muted-foreground text-xs">{label}</span>
      <span className="font-mono text-sm font-medium tabular-nums">{value}</span>
    </div>
  )
}

export function SyncStatusBar({ status, changesReceived }: { status: SyncStatus | null; changesReceived: number }) {
  const presentation =
    status === null
      ? { label: 'Opening local database', dot: 'bg-muted-foreground', pulse: true }
      : STATE_PRESENTATION[status.state]

  return (
    <section className="border-border bg-card/60 mt-6 flex flex-wrap items-center gap-x-6 gap-y-2 rounded-lg border px-4 py-3 shadow-xs">
      <div className="flex items-center gap-2">
        <span
          className={cn('size-2 rounded-full', presentation.dot, presentation.pulse && 'animate-status-pulse')}
          aria-hidden="true"
        />
        <span className="text-sm font-medium">{presentation.label}</span>
      </div>
      <Metric label="Queued to push" value={String(status?.pendingPushCount ?? 0)} />
      <Metric label="Changes from server" value={String(changesReceived)} />
      <Metric label="Server position" value={status?.lastPulledSeq?.toString() ?? '—'} />
      {status?.deviceId ? <Metric label="Device id" value={shortDeviceId(status.deviceId)} /> : null}
    </section>
  )
}

function shortDeviceId(deviceId: string): string {
  return deviceId.length > 10 ? `${deviceId.slice(0, 10)}…` : deviceId
}
