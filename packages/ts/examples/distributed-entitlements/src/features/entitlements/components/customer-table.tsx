import { CheckCircle2, CircleDashed, MousePointer2 } from 'lucide-react'
import { useCallback } from 'react'
import type { CustomerEntitlement } from '../../../lib/schemas'
import { formatCompactNumber, formatDateTime } from '../entitlements-utils'

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
  const body =
    customers.length === 0 ? (
      <tr>
        <td colSpan={8}>
          <EmptyLedger />
        </td>
      </tr>
    ) : (
      rows
    )

  return (
    <div className="table-wrap">
      <table className="ledger-table">
        <thead>
          <tr>
            <th>Account</th>
            <th>Plan</th>
            <th>Seats</th>
            <th>Quota</th>
            <th>Support</th>
            <th>Version</th>
            <th>Updated</th>
            <th>Focus</th>
          </tr>
        </thead>
        <tbody>{body}</tbody>
      </table>
    </div>
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
  const focusLabel = `Focus account for ${customer.name}`

  return (
    <tr className={selected ? 'selected' : undefined}>
      <td>
        <div className="customer-cell">
          <strong>{customer.name}</strong>
          <span>{customer.external_id}</span>
        </div>
      </td>
      <td>
        <span className={`plan-chip ${customer.plan}`}>{customer.plan}</span>
      </td>
      <td>{customer.seats}</td>
      <td>{formatCompactNumber(customer.api_quota)}</td>
      <td>{customer.support_tier}</td>
      <td>{customer.version}</td>
      <td>{formatDateTime(customer.updated_at)}</td>
      <td>
        <button
          className="row-icon-button"
          type="button"
          disabled={disabled}
          onClick={handleSelectClick}
          aria-label={focusLabel}
          aria-pressed={selected}
          title="Focus account"
        >
          {selected ? <CheckCircle2 size={16} /> : <MousePointer2 size={16} />}
        </button>
      </td>
    </tr>
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
