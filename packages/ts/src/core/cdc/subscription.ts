import type Database from 'better-sqlite3'
import type {
	ChangeEvent,
	Subscription,
	SubscriptionBuilder,
} from '../types.js'
import type { ChangeTracker } from './change-tracker.js'

interface InternalSubscription {
	id: number
	table: string
	filter: Record<string, unknown> | undefined
	callback: (event: ChangeEvent) => void
}

export class SubscriptionManager {
	private nextId = 1
	private readonly subscriptions = new Map<number, InternalSubscription>()
	private readonly byTable = new Map<string, Set<number>>()

	subscribe(
		table: string,
		filter: Record<string, unknown> | undefined,
		callback: (event: ChangeEvent) => void,
	): Subscription {
		const id = this.nextId++
		this.subscriptions.set(id, { id, table, filter, callback })

		let tableSet = this.byTable.get(table)
		if (!tableSet) {
			tableSet = new Set()
			this.byTable.set(table, tableSet)
		}
		tableSet.add(id)

		return {
			unsubscribe: () => {
				this.subscriptions.delete(id)
				const set = this.byTable.get(table)
				if (set) {
					set.delete(id)
					if (set.size === 0) {
						this.byTable.delete(table)
					}
				}
			},
		}
	}

	dispatch(events: ChangeEvent[]): void {
		for (const event of events) {
			const ids = this.byTable.get(event.table)
			if (!ids) continue

			for (const id of ids) {
				const sub = this.subscriptions.get(id)
				if (!sub) continue
				if (sub.filter && !matchesFilter(event, sub.filter)) {
					continue
				}
				try {
					sub.callback(event)
				} catch {
					/* subscriber errors must not break other subscribers */
				}
			}
		}
	}

	get size(): number {
		return this.subscriptions.size
	}

	subscriberCount(table: string): number {
		return this.byTable.get(table)?.size ?? 0
	}
}

export class SubscriptionBuilderImpl implements SubscriptionBuilder {
	private conditions: Record<string, unknown> | undefined

	constructor(
		private readonly table: string,
		private readonly manager: SubscriptionManager,
	) {}

	filter(conditions: Record<string, unknown>): SubscriptionBuilder {
		this.conditions = { ...this.conditions, ...conditions }
		return this
	}

	subscribe(callback: (event: ChangeEvent) => void): Subscription {
		return this.manager.subscribe(this.table, this.conditions, callback)
	}
}

export function startPolling(
	db: Database.Database,
	tracker: ChangeTracker,
	manager: SubscriptionManager,
	intervalMs: number,
): () => void {
	let running = true

	const stop = () => {
		running = false
		clearInterval(interval)
	}

	const tick = () => {
		if (!running) return
		try {
			const events = tracker.poll(db)
			if (events.length > 0) {
				manager.dispatch(events)
			}
		} catch {
			stop()
		}
	}

	const interval = setInterval(tick, intervalMs)
	interval.unref()

	return stop
}

function matchesFilter(
	event: ChangeEvent,
	filter: Record<string, unknown>,
): boolean {
	const target = event.type === 'delete' ? (event.oldRow ?? {}) : event.row
	for (const [key, value] of Object.entries(filter)) {
		if ((target as Record<string, unknown>)[key] !== value) {
			return false
		}
	}
	return true
}
