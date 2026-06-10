import { CheckCircle2, CircleDashed } from 'lucide-react'
import { useCallback } from 'react'
import type { CustomerEntitlement } from '../../../lib/schemas'
import { cn } from '../../../lib/ui'
import { formatCompactNumber } from '../entitlements-utils'
import { Badge } from './ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card'

export function CustomerTable({
  customers,
  selectedCustomer,
  pendingAction,
  onSelectCustomer,
}: {
  customers: CustomerEntitlement[]
  selectedCustomer: CustomerEntitlement | null
  pendingAction: string | null
  onSelectCustomer: (customer: CustomerEntitlement) => void
}) {
  const rows = customers.map(customer => (
    <CustomerListItem
      key={customer.id}
      customer={customer}
      selected={selectedCustomer?.id === customer.id}
      disabled={pendingAction !== null}
      onSelectCustomer={onSelectCustomer}
    />
  ))
  const body = customers.length === 0 ? <EmptyLedger /> : rows

  return (
    <Card className="customer-panel">
      <CardHeader>
        <div>
          <CardTitle>Accounts</CardTitle>
          <CardDescription>{customers.length} entitlement records</CardDescription>
        </div>
      </CardHeader>
      <CardContent className="customer-list">{body}</CardContent>
    </Card>
  )
}

function CustomerListItem({
  customer,
  selected,
  disabled,
  onSelectCustomer,
}: {
  customer: CustomerEntitlement
  selected: boolean
  disabled: boolean
  onSelectCustomer: (customer: CustomerEntitlement) => void
}) {
  const handleSelectClick = useCallback(() => {
    onSelectCustomer(customer)
  }, [customer, onSelectCustomer])

  return (
    <button
      className={cn('customer-row', selected && 'selected')}
      type="button"
      disabled={disabled}
      onClick={handleSelectClick}
      aria-pressed={selected}
    >
      <span className="customer-main">
        <strong>{customer.name}</strong>
        <span>{customer.external_id}</span>
      </span>
      <span className="customer-meta">
        <Badge variant={planVariant(customer.plan)}>{customer.plan}</Badge>
        <span>{formatCompactNumber(customer.api_quota)} quota</span>
      </span>
      {selected ? <CheckCircle2 className="customer-selected" size={16} /> : null}
    </button>
  )
}

export function EmptyLedger() {
  return (
    <div className="empty-state">
      <CircleDashed size={20} />
      <span>No entitlement records are available.</span>
    </div>
  )
}

function planVariant(plan: CustomerEntitlement['plan']): 'default' | 'secondary' | 'success' {
  if (plan === 'enterprise' || plan === 'scale') {
    return 'default'
  }
  if (plan === 'growth') {
    return 'success'
  }
  return 'secondary'
}
