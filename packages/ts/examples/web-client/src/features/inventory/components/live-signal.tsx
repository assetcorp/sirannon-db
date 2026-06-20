import { RadioTower } from 'lucide-react'

export function LiveSignal({ lastEvent, pendingAction }: { lastEvent: string; pendingAction: string | null }) {
  return (
    <div className="live-signal">
      <RadioTower size={18} />
      <output aria-live="polite">
        <strong>{pendingAction ?? 'CDC stream online'}</strong>
        <span>{lastEvent}</span>
      </output>
    </div>
  )
}
