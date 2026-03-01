import type { HookConfig } from '../types.js'
import type {
	HookDispose,
	HookEvent,
	HookEventContextMap,
	HookHandler,
} from './types.js'

type ErasedHookFn = (ctx: never) => void | Promise<void>

const HOOK_CONFIG_MAP: Record<keyof HookConfig, HookEvent> = {
	onBeforeQuery: 'beforeQuery',
	onAfterQuery: 'afterQuery',
	onBeforeConnect: 'beforeConnect',
	onDatabaseOpen: 'databaseOpen',
	onDatabaseClose: 'databaseClose',
	onBeforeSubscribe: 'beforeSubscribe',
}

export class HookRegistry {
	private hooks = new Map<HookEvent, ErasedHookFn[]>()

	constructor(config?: HookConfig) {
		if (config) {
			this.loadConfig(config)
		}
	}

	register<E extends HookEvent>(event: E, hook: HookHandler<E>): HookDispose {
		const stored = hook as ErasedHookFn
		this.addHook(event, stored)

		let disposed = false
		return () => {
			if (disposed) return
			disposed = true
			const current = this.hooks.get(event)
			if (!current) return
			const idx = current.indexOf(stored)
			if (idx !== -1) current.splice(idx, 1)
		}
	}

	async invoke<E extends HookEvent>(
		event: E,
		ctx: HookEventContextMap[E],
	): Promise<void> {
		const list = this.hooks.get(event)
		if (!list || list.length === 0) return

		const snapshot = list.slice() as HookHandler<E>[]
		for (const hook of snapshot) {
			await hook(ctx)
		}
	}

	invokeSync<E extends HookEvent>(event: E, ctx: HookEventContextMap[E]): void {
		const list = this.hooks.get(event)
		if (!list || list.length === 0) return

		const snapshot = list.slice() as HookHandler<E>[]
		for (const hook of snapshot) {
			const result = hook(ctx)
			if (
				result != null &&
				typeof (result as { then?: unknown }).then === 'function'
			) {
				throw new Error(
					`Hook for '${event}' returned a Promise. Use invoke() for async hooks.`,
				)
			}
		}
	}

	has(event: HookEvent): boolean {
		const list = this.hooks.get(event)
		return list !== undefined && list.length > 0
	}

	count(event: HookEvent): number {
		return this.hooks.get(event)?.length ?? 0
	}

	clear(event?: HookEvent): void {
		if (event) {
			this.hooks.delete(event)
		} else {
			this.hooks.clear()
		}
	}

	private addHook(event: HookEvent, hook: ErasedHookFn): void {
		const list = this.hooks.get(event)
		if (list) {
			list.push(hook)
		} else {
			this.hooks.set(event, [hook])
		}
	}

	private loadConfig(config: HookConfig): void {
		for (const [configKey, event] of Object.entries(HOOK_CONFIG_MAP)) {
			const value = config[configKey as keyof HookConfig]
			if (!value) continue

			const hooks = Array.isArray(value) ? value : [value]
			for (const hook of hooks) {
				this.addHook(event, hook as ErasedHookFn)
			}
		}
	}
}
