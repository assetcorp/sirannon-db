import type { CDCMetrics, ConnectionMetrics, MetricsConfig, QueryMetrics } from '../types.js'

export class MetricsCollector {
  private config: MetricsConfig

  constructor(config?: MetricsConfig) {
    this.config = config ?? {}
  }

  trackQuery<T>(fn: () => T, context: Omit<QueryMetrics, 'durationMs' | 'error'>): T {
    if (!this.config.onQueryComplete) {
      return fn()
    }

    const start = performance.now()
    let failed = false
    try {
      return fn()
    } catch (err) {
      failed = true
      throw err
    } finally {
      const durationMs = performance.now() - start
      try {
        this.config.onQueryComplete({
          ...context,
          durationMs,
          error: failed,
        })
      } catch {
        void 0
      }
    }
  }

  trackConnection(metrics: ConnectionMetrics): void {
    try {
      if (metrics.event === 'open') {
        this.config.onConnectionOpen?.(metrics)
      } else {
        this.config.onConnectionClose?.(metrics)
      }
    } catch {
      void 0
    }
  }

  trackCDCEvent(metrics: CDCMetrics): void {
    try {
      this.config.onCDCEvent?.(metrics)
    } catch {
      void 0
    }
  }

  get active(): boolean {
    return !!(
      this.config.onQueryComplete ||
      this.config.onConnectionOpen ||
      this.config.onConnectionClose ||
      this.config.onCDCEvent
    )
  }
}
