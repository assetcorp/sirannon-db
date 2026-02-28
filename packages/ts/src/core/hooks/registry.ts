import type { HookConfig } from '../types.js'
import type { HookEvent, HookFunction } from './types.js'

const HOOK_CONFIG_MAP: Record<keyof HookConfig, HookEvent> = {
	onBeforeQuery: 'beforeQuery',
	onAfterQuery: 'afterQuery',
	onBeforeConnect: 'beforeConnect',
	onDatabaseOpen: 'databaseOpen',
	onDatabaseClose: 'databaseClose',
	onBeforeSubscribe: 'beforeSubscribe',
}

export class HookRegistry {
	private hooks = new Map<HookEvent, HookFunction[]>()

	constructor(config?: HookConfig) {
		if (config) {
			this.loadConfig(config)
		}
	}

	register(event: HookEvent, hook: HookFunction): void {
		const list = this.hooks.get(event)
		if (list) {
			list.push(hook)
		} else {
			this.hooks.set(event, [hook])
		}
	}

	async invoke(event: HookEvent, ctx: unknown): Promise<void> {
		const list = this.hooks.get(event)
		if (!list) return

		for (const hook of list) {
			await hook(ctx)
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

	private loadConfig(config: HookConfig): void {
		for (const [configKey, event] of Object.entries(HOOK_CONFIG_MAP)) {
			const value = config[configKey as keyof HookConfig]
			if (!value) continue

			const hooks = Array.isArray(value) ? value : [value]
			for (const hook of hooks) {
				this.register(event, hook as HookFunction)
			}
		}
	}
}
