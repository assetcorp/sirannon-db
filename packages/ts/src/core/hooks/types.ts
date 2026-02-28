export type {
	QueryHookContext,
	BeforeQueryHook,
	AfterQueryHook,
	ConnectionHookContext,
	BeforeConnectHook,
	DatabaseOpenHook,
	DatabaseCloseHook,
	BeforeSubscribeHook,
	HookConfig,
} from '../types.js'

export type HookEvent =
	| 'beforeQuery'
	| 'afterQuery'
	| 'beforeConnect'
	| 'databaseOpen'
	| 'databaseClose'
	| 'beforeSubscribe'

export type HookFunction = (...args: unknown[]) => void | Promise<void>

export interface HookEventMap {
	beforeQuery: HookFunction
	afterQuery: HookFunction
	beforeConnect: HookFunction
	databaseOpen: HookFunction
	databaseClose: HookFunction
	beforeSubscribe: HookFunction
}
