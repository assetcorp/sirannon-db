import type { ProductStats } from '../types'

export function MetricsGrid({ stats }: { stats: ProductStats }) {
  return (
    <section className="metrics-grid">
      <Metric label="Catalog records" value={String(stats.totalProducts)} />
      <Metric label="Available units" value={stats.totalStock.toLocaleString()} />
      <Metric label="Reorder watch" value={String(stats.lowStock)} />
    </section>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}
