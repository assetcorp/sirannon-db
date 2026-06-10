import type { HTMLAttributes, ReactNode } from 'react'
import { cn } from '../../../../lib/ui'

type BadgeVariant = 'default' | 'secondary' | 'outline' | 'success' | 'warning' | 'destructive'

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  children: ReactNode
  variant?: BadgeVariant
}

export function Badge({ children, className, variant = 'default', ...props }: BadgeProps) {
  return (
    <span className={cn('ui-badge', `ui-badge-${variant}`, className)} {...props}>
      {children}
    </span>
  )
}
