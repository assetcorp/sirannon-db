import type { ReactNode } from 'react'
import { Label } from '@/components/ui/label'
import { SelectItem } from '@/components/ui/select'
import { cn } from '@/lib/utils'
import type { Plan, SupportTier } from '../../../lib/schemas'

export const PLAN_OPTIONS: Plan[] = ['free', 'growth', 'scale', 'enterprise']
export const SUPPORT_OPTIONS: SupportTier[] = ['community', 'standard', 'priority', 'named']

export function renderOption(value: string) {
  return (
    <SelectItem key={value} value={value}>
      {value}
    </SelectItem>
  )
}

export function FormField({
  id,
  label,
  children,
  className,
}: {
  id: string
  label: string
  children: ReactNode
  className?: string
}) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-1.5', className)}>
      <Label htmlFor={id} className="text-muted-foreground text-xs">
        {label}
      </Label>
      {children}
    </div>
  )
}
