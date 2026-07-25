import type { LucideIcon } from 'lucide-react'
import { cn } from '@/lib/utils'

export function CommandButton({
  icon: Icon,
  title,
  detail,
  disabled,
  dangerous = false,
  onClick,
}: {
  icon: LucideIcon
  title: string
  detail: string
  disabled: boolean
  dangerous?: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={cn(
        'group bg-background/40 hover:border-ring/40 hover:bg-muted/50 focus-visible:ring-ring/50 flex w-full items-start gap-3 rounded-lg border p-3 text-left transition-colors focus-visible:ring-[3px] focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50',
        dangerous && 'border-destructive/30 hover:border-destructive/50 hover:bg-destructive/10',
      )}
    >
      <span
        className={cn(
          'border-border/80 bg-muted/40 text-muted-foreground group-hover:text-foreground mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-md border transition-colors',
          dangerous && 'border-destructive/30 bg-destructive/10 text-destructive group-hover:text-destructive',
        )}
      >
        <Icon className="size-3.5" aria-hidden="true" />
      </span>
      <span className="flex min-w-0 flex-col gap-0.5">
        <span className={cn('text-sm font-medium', dangerous && 'text-destructive')}>{title}</span>
        <span className="text-muted-foreground text-xs">{detail}</span>
      </span>
    </button>
  )
}
