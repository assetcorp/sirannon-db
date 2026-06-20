import { CircleDashed } from 'lucide-react'
import { useCallback } from 'react'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import type { CustomerEntitlement } from '../../../lib/schemas'
import { formatCompactNumber } from '../entitlements-utils'
import { TONE_BADGE } from './status'

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
    <CustomerRow
      key={customer.id}
      customer={customer}
      selected={selectedCustomer?.id === customer.id}
      disabled={pendingAction !== null}
      onSelectCustomer={onSelectCustomer}
    />
  ))

  return (
    <Card className="gap-0 overflow-hidden py-0">
      <CardHeader className="border-border/70 border-b py-3.5">
        <CardTitle>Accounts</CardTitle>
        <CardDescription>{customers.length} entitlement records</CardDescription>
      </CardHeader>
      <CardContent className="p-0">
        <ScrollArea className="h-[480px]">
          {customers.length === 0 ? <EmptyAccounts /> : <div className="flex flex-col">{rows}</div>}
        </ScrollArea>
      </CardContent>
    </Card>
  )
}

function CustomerRow({
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
      type="button"
      disabled={disabled}
      onClick={handleSelectClick}
      aria-pressed={selected}
      className={cn(
        'group border-border/60 hover:bg-muted/50 focus-visible:ring-ring/50 relative flex w-full items-center justify-between gap-3 border-b px-4 py-3 text-left transition-colors last:border-b-0 focus-visible:ring-2 focus-visible:outline-none focus-visible:ring-inset disabled:pointer-events-none disabled:opacity-60',
        selected && 'bg-accent/60 hover:bg-accent/60',
      )}
    >
      {selected ? <span aria-hidden="true" className="bg-primary absolute inset-y-0 left-0 w-0.5" /> : null}
      <span className="flex min-w-0 flex-col gap-0.5">
        <span className="truncate text-sm font-medium">{customer.name}</span>
        <span className="text-muted-foreground truncate font-mono text-xs">{customer.external_id}</span>
      </span>
      <span className="flex shrink-0 flex-col items-end gap-1">
        <PlanBadge plan={customer.plan} />
        <span className="text-muted-foreground font-mono text-xs tabular-nums">
          {formatCompactNumber(customer.api_quota)} quota
        </span>
      </span>
    </button>
  )
}

export function PlanBadge({ plan }: { plan: CustomerEntitlement['plan'] }) {
  if (plan === 'enterprise' || plan === 'scale') {
    return <Badge className="font-mono text-[10px] uppercase">{plan}</Badge>
  }
  if (plan === 'growth') {
    return (
      <Badge variant="outline" className={cn('font-mono text-[10px] uppercase', TONE_BADGE.success)}>
        {plan}
      </Badge>
    )
  }
  return (
    <Badge variant="secondary" className="font-mono text-[10px] uppercase">
      {plan}
    </Badge>
  )
}

function EmptyAccounts() {
  return (
    <div className="text-muted-foreground flex h-[480px] flex-col items-center justify-center gap-2 px-6 text-center">
      <CircleDashed className="size-5" aria-hidden="true" />
      <span className="text-sm">No entitlement records are available.</span>
    </div>
  )
}
