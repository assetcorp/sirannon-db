import { Activity, CircleDashed, DatabaseZap, ReceiptText, ScrollText } from 'lucide-react'
import type { AuditRecord, BillingEvent, CustomerEntitlement, UsageEvent } from '../../../lib/schemas'
import { formatDateTime } from '../entitlements-utils'
import { Badge } from './ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card'

interface TimelineItem {
  id: string
  kind: 'usage' | 'billing' | 'audit'
  title: string
  detail: string
  meta: string
  outcome: 'accepted' | 'duplicate' | 'stale' | 'recorded'
  time: string
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
  const content = rows.length > 0 ? rows : <EmptyTimeline />

  return (
    <Card className="timeline-panel">
      <CardHeader>
        <div>
          <CardTitle>Activity</CardTitle>
          <CardDescription>{selectedCustomer ? selectedCustomer.name : 'No account selected'}</CardDescription>
        </div>
        <Badge variant="outline">{items.length} events</Badge>
      </CardHeader>
      <CardContent className="timeline-list">{content}</CardContent>
    </Card>
  )
}

function TimelineRow({ item }: { item: TimelineItem }) {
  return (
    <article className="timeline-row">
      <span className="timeline-icon">{iconForKind(item.kind)}</span>
      <div className="timeline-copy">
        <div className="timeline-head">
          <strong>{item.title}</strong>
          <Badge variant={badgeForOutcome(item.outcome)}>{item.outcome}</Badge>
        </div>
        <p>{item.detail}</p>
        <span>{item.meta}</span>
      </div>
      <time>{formatDateTime(item.time)}</time>
    </article>
  )
}

function EmptyTimeline() {
  return (
    <div className="empty-state timeline-empty">
      <CircleDashed size={20} />
      <span>No activity is available for this account.</span>
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

function iconForKind(kind: TimelineItem['kind']) {
  if (kind === 'usage') {
    return <DatabaseZap size={16} />
  }
  if (kind === 'billing') {
    return <ReceiptText size={16} />
  }
  if (kind === 'audit') {
    return <ScrollText size={16} />
  }
  return <Activity size={16} />
}

function badgeForOutcome(outcome: TimelineItem['outcome']): 'success' | 'secondary' | 'warning' | 'destructive' {
  if (outcome === 'accepted' || outcome === 'recorded') {
    return 'success'
  }
  if (outcome === 'stale') {
    return 'warning'
  }
  if (outcome === 'duplicate') {
    return 'secondary'
  }
  return 'destructive'
}
