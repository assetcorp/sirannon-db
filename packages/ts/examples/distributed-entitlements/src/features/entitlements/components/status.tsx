import { cn } from '@/lib/utils'

export type StatusTone = 'success' | 'warning' | 'destructive' | 'neutral'

const TONE_DOT: Record<StatusTone, string> = {
  success: 'bg-success',
  warning: 'bg-warning',
  destructive: 'bg-destructive',
  neutral: 'bg-muted-foreground',
}

export const TONE_BADGE: Record<StatusTone, string> = {
  success: 'border-success/40 bg-success/10 text-success',
  warning: 'border-warning/40 bg-warning/10 text-warning',
  destructive: 'border-destructive/40 bg-destructive/10 text-destructive',
  neutral: 'border-border bg-muted/40 text-muted-foreground',
}

export function StatusDot({
  tone,
  pulse = false,
  className,
}: {
  tone: StatusTone
  pulse?: boolean
  className?: string
}) {
  return (
    <span
      aria-hidden="true"
      className={cn(
        'inline-flex size-2 shrink-0 rounded-full',
        TONE_DOT[tone],
        pulse && 'animate-status-pulse motion-reduce:animate-none',
        className,
      )}
    />
  )
}
