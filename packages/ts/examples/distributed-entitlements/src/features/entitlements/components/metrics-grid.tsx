import { KeyRound, ServerCrash, Users, WalletCards } from 'lucide-react'
import { formatCompactNumber } from '../entitlements-utils'
import type { ControlPlaneStats } from '../types'

export function MetricsGrid({ stats }: { stats: ControlPlaneStats }) {
  return (
    <section className="metrics-grid">
      <MetricCard icon={<Users size={18} />} label="Active Customers" value={String(stats.activeCustomers)} />
      <MetricCard
        icon={<WalletCards size={18} />}
        label="Allocated Seats"
        value={formatCompactNumber(stats.totalSeats)}
      />
      <MetricCard icon={<KeyRound size={18} />} label="Quota Remaining" value={formatCompactNumber(stats.totalQuota)} />
      <MetricCard
        icon={<ServerCrash size={18} />}
        label="Primary / Down"
        value={`${stats.primaryNode} / ${stats.unavailableNodes}`}
      />
    </section>
  )
}

function MetricCard({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div className="metric-card">
      <span className="metric-icon">{icon}</span>
      <span className="metric-label">{label}</span>
      <strong>{value}</strong>
    </div>
  )
}
