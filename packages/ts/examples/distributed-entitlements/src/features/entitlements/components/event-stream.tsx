import type { ReactNode } from 'react'
import type { AuditRecord, BillingEvent, UsageEvent } from '../../../lib/schemas'
import { formatDateTime } from '../entitlements-utils'
import { PanelHeader } from './panel-header'

type EventStreamProps =
  | {
      icon: ReactNode
      title: string
      kind: 'billing'
      records: BillingEvent[]
    }
  | {
      icon: ReactNode
      title: string
      kind: 'audit'
      records: AuditRecord[]
    }
  | {
      icon: ReactNode
      title: string
      kind: 'usage'
      records: UsageEvent[]
    }

export function EventStream(props: EventStreamProps) {
  const rows =
    props.kind === 'billing'
      ? props.records.map(record => <BillingEventRow key={record.id} record={record} />)
      : props.kind === 'usage'
        ? props.records.map(record => <UsageEventRow key={record.id} record={record} />)
        : props.records.map(record => <AuditRecordRow key={record.id} record={record} />)

  return (
    <div className="ops-panel">
      <PanelHeader icon={props.icon} title={props.title} />
      <div className="event-list">{rows}</div>
    </div>
  )
}

function UsageEventRow({ record }: { record: UsageEvent }) {
  return (
    <article className="event-row">
      <div>
        <strong>{record.customer_name}</strong>
        <span>{record.idempotency_key}</span>
      </div>
      <div className="event-meta">
        <span>{record.units} units</span>
        <span>{record.source}</span>
        <span>{formatDateTime(record.created_at)}</span>
      </div>
    </article>
  )
}

function BillingEventRow({ record }: { record: BillingEvent }) {
  return (
    <article className="event-row">
      <div>
        <strong>{record.event_type}</strong>
        <span>{record.customer_external_id}</span>
      </div>
      <div className="event-meta">
        <span>v{record.version}</span>
        <span>{record.outcome}</span>
        <span>{formatDateTime(record.processed_at)}</span>
      </div>
    </article>
  )
}

function AuditRecordRow({ record }: { record: AuditRecord }) {
  return (
    <article className="event-row">
      <div>
        <strong>{record.action}</strong>
        <span>{record.detail}</span>
      </div>
      <div className="event-meta">
        <span>{record.actor}</span>
        <span>{record.target}</span>
        <span>{formatDateTime(record.created_at)}</span>
      </div>
    </article>
  )
}
