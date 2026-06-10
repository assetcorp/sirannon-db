import { CalendarClock, KeyRound, Layers3, UserRound, WalletCards } from 'lucide-react'
import type { CustomerEntitlement } from '../../../lib/schemas'
import { formatCompactNumber, formatDateTime } from '../entitlements-utils'
import { Badge } from './ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card'

export function EntitlementDetail({ customer }: { customer: CustomerEntitlement | null }) {
  if (!customer) {
    return (
      <Card className="entitlement-detail empty-detail">
        <CardContent>
          <UserRound size={22} />
          <span>Select an account to inspect entitlement state.</span>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className="entitlement-detail">
      <CardHeader>
        <div>
          <CardTitle>{customer.name}</CardTitle>
          <CardDescription>{customer.external_id}</CardDescription>
        </div>
        <Badge variant={customer.active === 1 && customer.status === 'active' ? 'success' : 'warning'}>
          {customer.status}
        </Badge>
      </CardHeader>
      <CardContent>
        <div className="entitlement-hero">
          <div>
            <span>Plan</span>
            <strong>{customer.plan}</strong>
          </div>
          <div>
            <span>Quota</span>
            <strong>{formatCompactNumber(customer.api_quota)}</strong>
          </div>
        </div>
        <div className="entitlement-grid">
          <DetailStat icon={<WalletCards size={16} />} label="Seats" value={String(customer.seats)} />
          <DetailStat icon={<KeyRound size={16} />} label="Version" value={String(customer.version)} />
          <DetailStat icon={<Layers3 size={16} />} label="Support" value={customer.support_tier} />
          <DetailStat icon={<CalendarClock size={16} />} label="Updated" value={formatDateTime(customer.updated_at)} />
        </div>
      </CardContent>
    </Card>
  )
}

function DetailStat({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div className="detail-stat">
      <span>{icon}</span>
      <div>
        <em>{label}</em>
        <strong>{value}</strong>
      </div>
    </div>
  )
}
