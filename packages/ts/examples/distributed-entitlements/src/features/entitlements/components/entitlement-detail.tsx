import { UserRound } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Separator } from '@/components/ui/separator'
import { cn } from '@/lib/utils'
import type { CustomerEntitlement } from '../../../lib/schemas'
import { formatCompactNumber, formatDateTime } from '../entitlements-utils'
import { type StatusTone, TONE_BADGE } from './status'

export function EntitlementDetail({ customer }: { customer: CustomerEntitlement | null }) {
  if (!customer) {
    return (
      <Card>
        <CardContent className="text-muted-foreground flex flex-col items-center justify-center gap-2 py-10 text-center">
          <UserRound className="size-5" aria-hidden="true" />
          <span className="text-sm">Select an account to inspect entitlement state.</span>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>{customer.name}</CardTitle>
        <CardDescription className="font-mono text-xs">{customer.external_id}</CardDescription>
        <CardAction>
          <EntitlementStatusBadge customer={customer} />
        </CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <div className="flex items-center gap-5">
          <HeroStat label="Plan" value={customer.plan} className="capitalize" />
          <Separator orientation="vertical" className="h-10" />
          <HeroStat label="API quota" value={formatCompactNumber(customer.api_quota)} mono />
          <Separator orientation="vertical" className="h-10" />
          <HeroStat label="Billing version" value={`v${customer.version}`} mono />
        </div>
        <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-4">
          <DetailStat label="Seats" value={String(customer.seats)} mono />
          <DetailStat label="Support tier" value={customer.support_tier} className="capitalize" />
          <DetailStat label="Created" value={formatDateTime(customer.created_at)} />
          <DetailStat label="Updated" value={formatDateTime(customer.updated_at)} />
        </div>
      </CardContent>
    </Card>
  )
}

function EntitlementStatusBadge({ customer }: { customer: CustomerEntitlement }) {
  const tone = statusTone(customer)
  return (
    <Badge variant="outline" className={cn('font-mono text-[10px] uppercase', TONE_BADGE[tone])}>
      {customer.status.replace(/_/g, ' ')}
    </Badge>
  )
}

function statusTone(customer: CustomerEntitlement): StatusTone {
  if (customer.status === 'suspended') {
    return 'destructive'
  }
  if (customer.status === 'past_due' || customer.active !== 1) {
    return 'warning'
  }
  return 'success'
}

function HeroStat({
  label,
  value,
  mono = false,
  className,
}: {
  label: string
  value: string
  mono?: boolean
  className?: string
}) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <span className="text-muted-foreground text-xs font-medium tracking-wider uppercase">{label}</span>
      <span
        className={cn('truncate text-xl font-semibold tracking-tight', mono && 'font-mono tabular-nums', className)}
      >
        {value}
      </span>
    </div>
  )
}

function DetailStat({
  label,
  value,
  mono = false,
  className,
}: {
  label: string
  value: string
  mono?: boolean
  className?: string
}) {
  return (
    <div className="border-border/70 bg-background/40 flex min-w-0 flex-col gap-0.5 rounded-lg border px-2.5 py-2">
      <span className="text-muted-foreground text-[10px] font-medium tracking-wider uppercase">{label}</span>
      <span className={cn('truncate text-sm font-medium', mono && 'font-mono tabular-nums', className)}>{value}</span>
    </div>
  )
}
