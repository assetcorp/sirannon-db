import { CircleDashed, DatabaseZap, type LucideIcon, ReceiptText, ScrollText } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import type { AuditRecord, BillingEvent, CustomerEntitlement, UsageEvent } from '../../../lib/schemas'
import { formatDateTime } from '../entitlements-utils'
import { type StatusTone, TONE_BADGE } from './status'

interface TimelineItem {
  id: string
  kind: 'usage' | 'billing' | 'audit'
  title: string
  detail: string
  meta: string
  outcome: 'accepted' | 'duplicate' | 'stale' | 'recorded'
  time: string
}

const KIND_ICON: Record<TimelineItem['kind'], LucideIcon> = {
  usage: DatabaseZap,
  billing: ReceiptText,
  audit: ScrollText,
}

const OUTCOME_TONE: Record<TimelineItem['outcome'], StatusTone> = {
  accepted: 'success',
  recorded: 'success',
  duplicate: 'neutral',
  stale: 'warning',
}

export function ActivityTimeline({
  selectedCustomer,
  usage,
  billingEvents,
  auditLog,
}: {
  selectedCustomer: CustomerEntitlement | null
  usage: UsageEvent[]
  billingEvents: BillingEvent[]
  auditLog: AuditRecord[]
}) {
  const items = selectedCustomer ? timelineForCustomer(selectedCustomer, usage, billingEvents, auditLog) : []
  const rows = items.map(item => <TimelineRow key={item.id} item={item} />)

  return (
    <Card className="gap-0 overflow-hidden py-0">
      <CardHeader className="border-border/70 border-b py-3.5">
        <CardTitle>Activity</CardTitle>
        <CardDescription>{selectedCustomer ? selectedCustomer.name : 'No account selected'}</CardDescription>
        <CardAction>
          <Badge variant="outline" className="font-mono text-xs tabular-nums">
            {items.length} events
          </Badge>
        </CardAction>
      </CardHeader>
      <CardContent className="p-0">
        <ScrollArea className="h-[360px]">
          {rows.length > 0 ? (
            <div role="log" aria-live="polite" aria-relevant="additions" className="flex flex-col px-4">
              {rows}
            </div>
          ) : (
            <EmptyTimeline />
          )}
        </ScrollArea>
      </CardContent>
    </Card>
  )
}

function TimelineRow({ item }: { item: TimelineItem }) {
  const Icon = KIND_ICON[item.kind]
  return (
    <article className="border-border/60 flex items-start gap-3 border-b py-3 last:border-b-0">
      <span className="border-border/80 bg-muted/40 text-muted-foreground mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-md border">
        <Icon className="size-3.5" aria-hidden="true" />
      </span>
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-medium">{item.title}</span>
          <Badge variant="outline" className={cn('font-mono text-[10px]', TONE_BADGE[OUTCOME_TONE[item.outcome]])}>
            {item.outcome}
          </Badge>
        </div>
        <p className="text-muted-foreground truncate font-mono text-xs">{item.detail}</p>
        <span className="text-muted-foreground/80 text-xs">{item.meta}</span>
      </div>
      <time dateTime={item.time} className="text-muted-foreground shrink-0 pt-0.5 font-mono text-[11px] tabular-nums">
        {formatDateTime(item.time)}
      </time>
    </article>
  )
}

function EmptyTimeline() {
  return (
    <div className="text-muted-foreground flex h-[360px] flex-col items-center justify-center gap-2 px-6 text-center">
      <CircleDashed className="size-5" aria-hidden="true" />
      <span className="text-sm">No activity is available for this account.</span>
    </div>
  )
}

function timelineForCustomer(
  customer: CustomerEntitlement,
  usage: UsageEvent[],
  billingEvents: BillingEvent[],
  auditLog: AuditRecord[],
): TimelineItem[] {
  const customerId = String(customer.id)
  const usageItems = usage
    .filter(record => record.customer_id === customer.id)
    .map(record => ({
      id: `usage-${record.id}`,
      kind: 'usage' as const,
      title: `${record.units} API units`,
      detail: record.idempotency_key,
      meta: record.source,
      outcome: 'recorded' as const,
      time: record.created_at,
    }))
  const billingItems = billingEvents
    .filter(record => record.customer_external_id === customer.external_id)
    .map(record => ({
      id: `billing-${record.id}`,
      kind: 'billing' as const,
      title: record.event_type,
      detail: record.provider_event_id,
      meta: `version ${record.version}`,
      outcome: record.outcome,
      time: record.processed_at,
    }))
  const auditItems = auditLog
    .filter(record => record.target === customer.external_id || record.target === customerId)
    .map(record => ({
      id: `audit-${record.id}`,
      kind: 'audit' as const,
      title: record.action,
      detail: record.detail,
      meta: record.actor,
      outcome: 'recorded' as const,
      time: record.created_at,
    }))

  return [...usageItems, ...billingItems, ...auditItems].sort(compareTimelineItems)
}

function compareTimelineItems(a: TimelineItem, b: TimelineItem): number {
  const leftTime = Date.parse(a.time)
  const rightTime = Date.parse(b.time)
  if (Number.isNaN(leftTime) && Number.isNaN(rightTime)) {
    return 0
  }
  if (Number.isNaN(leftTime)) {
    return 1
  }
  if (Number.isNaN(rightTime)) {
    return -1
  }
  return rightTime - leftTime
}
