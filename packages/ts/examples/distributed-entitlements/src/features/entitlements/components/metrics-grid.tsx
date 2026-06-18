import { KeyRound, type LucideIcon, RadioTower, Users, WalletCards } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { formatCompactNumber } from '../entitlements-utils'
import type { ControlPlaneStats } from '../types'

export function MetricsGrid({ stats }: { stats: ControlPlaneStats }) {
  return (
    <section aria-label="Control plane metrics" className="grid grid-cols-2 gap-3 xl:grid-cols-4">
      <MetricCard icon={Users} label="Active accounts" value={String(stats.activeCustomers)} />
      <MetricCard icon={WalletCards} label="Seats" value={formatCompactNumber(stats.totalSeats)} />
      <MetricCard icon={KeyRound} label="API quota" value={formatCompactNumber(stats.totalQuota)} />
      <MetricCard icon={RadioTower} label="Primary node" value={stats.primaryNode} />
    </section>
  )
}

function MetricCard({ icon: Icon, label, value }: { icon: LucideIcon; label: string; value: string }) {
  return (
    <Card size="sm">
      <CardContent className="flex flex-col gap-1.5">
        <span className="text-muted-foreground flex items-center gap-1.5 text-xs font-medium tracking-wider uppercase">
          <Icon className="size-3.5 shrink-0" aria-hidden="true" />
          <span className="truncate">{label}</span>
        </span>
        <span className="truncate font-mono text-2xl font-semibold tracking-tight tabular-nums">{value}</span>
      </CardContent>
    </Card>
  )
}
