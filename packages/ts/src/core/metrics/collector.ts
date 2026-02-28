import type {
	MetricsConfig,
	QueryMetrics,
	ConnectionMetrics,
	CDCMetrics,
} from '../types.js'

export class MetricsCollector {
	private config: MetricsConfig

	constructor(config?: MetricsConfig) {
		this.config = config ?? {}
	}

	trackQuery<T>(
		fn: () => T,
		context: Omit<QueryMetrics, 'durationMs'>,
	): T {
		if (!this.config.onQueryComplete) {
			return fn()
		}

		const start = performance.now()
		try {
			const result = fn()
			const durationMs = performance.now() - start
			this.config.onQueryComplete({ ...context, durationMs })
			return result
		} catch (error) {
			const durationMs = performance.now() - start
			this.config.onQueryComplete({ ...context, durationMs })
			throw error
		}
	}

	trackConnection(metrics: ConnectionMetrics): void {
		if (metrics.event === 'open') {
			this.config.onConnectionOpen?.(metrics)
		} else {
			this.config.onConnectionClose?.(metrics)
		}
	}

	trackCDCEvent(metrics: CDCMetrics): void {
		this.config.onCDCEvent?.(metrics)
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
