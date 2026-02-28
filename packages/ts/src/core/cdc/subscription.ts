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

	subscribe(
		table: string,
		filter: Record<string, unknown> | undefined,
		callback: (event: ChangeEvent) => void,
	): Subscription {
		const id = this.nextId++
		this.subscriptions.set(id, { id, table, filter, callback })

		return {
			unsubscribe: () => {
				this.subscriptions.delete(id)
			},
		}
	}

	dispatch(events: ChangeEvent[]): void {
		for (const event of events) {
			for (const sub of this.subscriptions.values()) {
				if (sub.table !== event.table) {
					continue
				}
				if (sub.filter && !matchesFilter(event, sub.filter)) {
					continue
				}
				sub.callback(event)
			}
		}
	}

	get size(): number {
		return this.subscriptions.size
	}

	subscriberCount(table: string): number {
		let count = 0
		for (const sub of this.subscriptions.values()) {
			if (sub.table === table) {
				count++
			}
		}
		return count
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

	const tick = () => {
		if (!running) return
		const events = tracker.poll(db)
		if (events.length > 0) {
			manager.dispatch(events)
		}
	}

	const interval = setInterval(tick, intervalMs)

	return () => {
		running = false
		clearInterval(interval)
	}
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
