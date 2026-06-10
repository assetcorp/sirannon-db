import { KeyRound, ServerCrash, Users, WalletCards } from 'lucide-react'
import { formatCompactNumber } from '../entitlements-utils'
import type { ControlPlaneStats } from '../types'
import { Card, CardContent } from './ui/card'

export function MetricsGrid({ stats }: { stats: ControlPlaneStats }) {
  return (
    <section className="metrics-grid" aria-label="Control plane metrics">
      <MetricCard icon={<Users size={17} />} label="Accounts" value={String(stats.activeCustomers)} />
      <MetricCard icon={<WalletCards size={17} />} label="Seats" value={formatCompactNumber(stats.totalSeats)} />
      <MetricCard icon={<KeyRound size={17} />} label="Quota" value={formatCompactNumber(stats.totalQuota)} />
      <MetricCard icon={<ServerCrash size={17} />} label="Primary" value={stats.primaryNode} />
    </section>
  )
}

function MetricCard({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <Card className="metric-card">
      <CardContent>
        <span className="metric-icon">{icon}</span>
        <span className="metric-label">{label}</span>
        <strong>{value}</strong>
      </CardContent>
    </Card>
  )
}
