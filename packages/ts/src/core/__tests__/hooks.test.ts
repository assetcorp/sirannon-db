import { describe, it, expect, vi } from 'vitest'
import { HookRegistry } from '../hooks/registry.js'
import type { HookConfig, QueryHookContext } from '../types.js'
import type { HookDispose } from '../hooks/types.js'

describe('HookRegistry', () => {
	const queryCtx: QueryHookContext = {
		databaseId: 'main',
		sql: 'SELECT 1',
	}

	describe('register and invoke', () => {
		it('invokes a registered hook with the provided context', async () => {
			const registry = new HookRegistry()
			const hook = vi.fn()

			registry.register('beforeQuery', hook)
			await registry.invoke('beforeQuery', queryCtx)

			expect(hook).toHaveBeenCalledOnce()
			expect(hook).toHaveBeenCalledWith(queryCtx)
		})

		it('invokes multiple hooks in registration order', async () => {
			const registry = new HookRegistry()
			const order: number[] = []

			registry.register('beforeQuery', () => {
				order.push(1)
			})
			registry.register('beforeQuery', () => {
				order.push(2)
			})
			registry.register('beforeQuery', () => {
				order.push(3)
			})

			await registry.invoke('beforeQuery', queryCtx)

			expect(order).toEqual([1, 2, 3])
		})

		it('does nothing when invoking an event with no hooks', async () => {
			const registry = new HookRegistry()
			await expect(
				registry.invoke('beforeQuery', queryCtx),
			).resolves.toBeUndefined()
		})

		it('keeps hooks for different events independent', async () => {
			const registry = new HookRegistry()
			const beforeQuery = vi.fn()
			const afterQuery = vi.fn()

			registry.register('beforeQuery', beforeQuery)
			registry.register('afterQuery', afterQuery)

			await registry.invoke('beforeQuery', queryCtx)

			expect(beforeQuery).toHaveBeenCalledOnce()
			expect(afterQuery).not.toHaveBeenCalled()
		})
	})

	describe('dispose', () => {
		it('returns a dispose function that removes the hook', async () => {
			const registry = new HookRegistry()
			const hook = vi.fn()

			const dispose = registry.register('beforeQuery', hook)
			dispose()

			await registry.invoke('beforeQuery', queryCtx)
			expect(hook).not.toHaveBeenCalled()
		})

		it('is idempotent when called multiple times', () => {
			const registry = new HookRegistry()
			const hook = vi.fn()

			const dispose = registry.register('beforeQuery', hook)
			dispose()
			dispose()
			dispose()

			expect(registry.count('beforeQuery')).toBe(0)
		})

		it('removes only the specific hook, leaving others intact', async () => {
			const registry = new HookRegistry()
			const hookA = vi.fn()
			const hookB = vi.fn()

			const disposeA = registry.register('beforeQuery', hookA)
			registry.register('beforeQuery', hookB)

			disposeA()
			await registry.invoke('beforeQuery', queryCtx)

			expect(hookA).not.toHaveBeenCalled()
			expect(hookB).toHaveBeenCalledOnce()
		})

		it('reflects in has and count after disposal', () => {
			const registry = new HookRegistry()
			const dispose = registry.register(
				'beforeQuery',
				vi.fn(),
			)

			expect(registry.has('beforeQuery')).toBe(true)
			expect(registry.count('beforeQuery')).toBe(1)

			dispose()

			expect(registry.has('beforeQuery')).toBe(false)
			expect(registry.count('beforeQuery')).toBe(0)
		})
	})

	describe('async hooks', () => {
		it('awaits async hooks before proceeding to the next one', async () => {
			const registry = new HookRegistry()
			const order: number[] = []

			registry.register('beforeQuery', async () => {
				await new Promise(r => setTimeout(r, 10))
				order.push(1)
			})
			registry.register('beforeQuery', () => {
				order.push(2)
			})

			await registry.invoke('beforeQuery', queryCtx)

			expect(order).toEqual([1, 2])
		})
	})

	describe('throw-to-deny', () => {
		it('propagates errors thrown by sync hooks', async () => {
			const registry = new HookRegistry()
			registry.register('beforeQuery', () => {
				throw new Error('access denied')
			})

			await expect(
				registry.invoke('beforeQuery', queryCtx),
			).rejects.toThrow('access denied')
		})

		it('propagates errors thrown by async hooks', async () => {
			const registry = new HookRegistry()
			registry.register('beforeQuery', async () => {
				throw new Error('async denial')
			})

			await expect(
				registry.invoke('beforeQuery', queryCtx),
			).rejects.toThrow('async denial')
		})

		it('stops invoking remaining hooks after one throws', async () => {
			const registry = new HookRegistry()
			const secondHook = vi.fn()

			registry.register('beforeQuery', () => {
				throw new Error('denied')
			})
			registry.register('beforeQuery', secondHook)

			await expect(
				registry.invoke('beforeQuery', queryCtx),
			).rejects.toThrow('denied')
			expect(secondHook).not.toHaveBeenCalled()
		})
	})

	describe('snapshot isolation', () => {
		it('hooks registered during invocation do not run in the current cycle', async () => {
			const registry = new HookRegistry()
			const lateHook = vi.fn()

			registry.register('beforeQuery', () => {
				registry.register('beforeQuery', lateHook)
			})

			await registry.invoke('beforeQuery', queryCtx)
			expect(lateHook).not.toHaveBeenCalled()

			await registry.invoke('beforeQuery', queryCtx)
			expect(lateHook).toHaveBeenCalledOnce()
		})

		it('hooks disposed during invocation still run in the current cycle', async () => {
			const registry = new HookRegistry()
			const order: number[] = []
			let dispose2: HookDispose

			registry.register('beforeQuery', () => {
				order.push(1)
				dispose2()
			})

			dispose2 = registry.register('beforeQuery', () => {
				order.push(2)
			})

			await registry.invoke('beforeQuery', queryCtx)
			expect(order).toEqual([1, 2])

			order.length = 0
			await registry.invoke('beforeQuery', queryCtx)
			expect(order).toEqual([1])
		})
	})

	describe('duplicate registration', () => {
		it('calls the same function twice when registered twice', async () => {
			const registry = new HookRegistry()
			const hook = vi.fn()

			registry.register('beforeQuery', hook)
			registry.register('beforeQuery', hook)

			await registry.invoke('beforeQuery', queryCtx)
			expect(hook).toHaveBeenCalledTimes(2)
		})

		it('disposes only one instance when the same function is registered twice', async () => {
			const registry = new HookRegistry()
			const hook = vi.fn()

			const dispose1 = registry.register('beforeQuery', hook)
			registry.register('beforeQuery', hook)

			dispose1()
			await registry.invoke('beforeQuery', queryCtx)
			expect(hook).toHaveBeenCalledOnce()
		})
	})

	describe('invokeSync', () => {
		it('runs sync hooks in registration order', () => {
			const registry = new HookRegistry()
			const order: number[] = []

			registry.register('beforeQuery', () => {
				order.push(1)
			})
			registry.register('beforeQuery', () => {
				order.push(2)
			})

			registry.invokeSync('beforeQuery', queryCtx)
			expect(order).toEqual([1, 2])
		})

		it('does nothing when no hooks are registered', () => {
			const registry = new HookRegistry()
			expect(() =>
				registry.invokeSync('beforeQuery', queryCtx),
			).not.toThrow()
		})

		it('throws when a hook returns a Promise', () => {
			const registry = new HookRegistry()
			registry.register('beforeQuery', async () => {})

			expect(() =>
				registry.invokeSync('beforeQuery', queryCtx),
			).toThrow(
				"Hook for 'beforeQuery' returned a Promise",
			)
		})

		it('propagates sync hook errors', () => {
			const registry = new HookRegistry()
			registry.register('beforeQuery', () => {
				throw new Error('sync denial')
			})

			expect(() =>
				registry.invokeSync('beforeQuery', queryCtx),
			).toThrow('sync denial')
		})

		it('snapshots the array so mutations during invocation are safe', () => {
			const registry = new HookRegistry()
			const lateHook = vi.fn()

			registry.register('beforeQuery', () => {
				registry.register('beforeQuery', lateHook)
			})

			registry.invokeSync('beforeQuery', queryCtx)
			expect(lateHook).not.toHaveBeenCalled()

			registry.invokeSync('beforeQuery', queryCtx)
			expect(lateHook).toHaveBeenCalledOnce()
		})

		it('disposed hooks still run in the current sync cycle', () => {
			const registry = new HookRegistry()
			const order: number[] = []
			let dispose2: HookDispose

			registry.register('beforeQuery', () => {
				order.push(1)
				dispose2()
			})

			dispose2 = registry.register('beforeQuery', () => {
				order.push(2)
			})

			registry.invokeSync('beforeQuery', queryCtx)
			expect(order).toEqual([1, 2])

			order.length = 0
			registry.invokeSync('beforeQuery', queryCtx)
			expect(order).toEqual([1])
		})
	})

	describe('initialization from HookConfig', () => {
		it('loads single hooks from config', async () => {
			const beforeQuery = vi.fn()
			const afterQuery = vi.fn()
			const config: HookConfig = {
				onBeforeQuery: beforeQuery,
				onAfterQuery: afterQuery,
			}

			const registry = new HookRegistry(config)

			await registry.invoke('beforeQuery', queryCtx)
			await registry.invoke('afterQuery', {
				...queryCtx,
				durationMs: 1,
			})

			expect(beforeQuery).toHaveBeenCalledOnce()
			expect(afterQuery).toHaveBeenCalledOnce()
		})

		it('loads hook arrays from config', async () => {
			const hook1 = vi.fn()
			const hook2 = vi.fn()
			const config: HookConfig = {
				onBeforeQuery: [hook1, hook2],
			}

			const registry = new HookRegistry(config)
			await registry.invoke('beforeQuery', queryCtx)

			expect(hook1).toHaveBeenCalledOnce()
			expect(hook2).toHaveBeenCalledOnce()
		})

		it('maps all config keys to the correct event names', async () => {
			const hooks = {
				onBeforeQuery: vi.fn(),
				onAfterQuery: vi.fn(),
				onBeforeConnect: vi.fn(),
				onDatabaseOpen: vi.fn(),
				onDatabaseClose: vi.fn(),
				onBeforeSubscribe: vi.fn(),
			}

			const registry = new HookRegistry(hooks)

			await registry.invoke('beforeQuery', queryCtx)
			await registry.invoke('afterQuery', {
				...queryCtx,
				durationMs: 1,
			})
			await registry.invoke('beforeConnect', {
				databaseId: 'main',
				path: '/data/main.db',
			})
			await registry.invoke('databaseOpen', {
				databaseId: 'main',
				path: '/data/main.db',
			})
			await registry.invoke('databaseClose', {
				databaseId: 'main',
				path: '/data/main.db',
			})
			await registry.invoke('beforeSubscribe', {
				databaseId: 'main',
				table: 'users',
			})

			for (const hook of Object.values(hooks)) {
				expect(hook).toHaveBeenCalledOnce()
			}
		})
	})

	describe('has and count', () => {
		it('reports whether hooks are registered for an event', () => {
			const registry = new HookRegistry()

			expect(registry.has('beforeQuery')).toBe(false)
			registry.register('beforeQuery', vi.fn())
			expect(registry.has('beforeQuery')).toBe(true)
		})

		it('reports the number of hooks registered for an event', () => {
			const registry = new HookRegistry()

			expect(registry.count('beforeQuery')).toBe(0)
			registry.register('beforeQuery', vi.fn())
			registry.register('beforeQuery', vi.fn())
			expect(registry.count('beforeQuery')).toBe(2)
		})
	})

	describe('clear', () => {
		it('clears hooks for a specific event', async () => {
			const registry = new HookRegistry()
			const hook = vi.fn()

			registry.register('beforeQuery', hook)
			registry.clear('beforeQuery')
			await registry.invoke('beforeQuery', queryCtx)

			expect(hook).not.toHaveBeenCalled()
		})

		it('clears all hooks when called without arguments', async () => {
			const registry = new HookRegistry()
			const beforeQuery = vi.fn()
			const afterQuery = vi.fn()

			registry.register('beforeQuery', beforeQuery)
			registry.register('afterQuery', afterQuery)
			registry.clear()

			await registry.invoke('beforeQuery', queryCtx)
			await registry.invoke('afterQuery', {
				...queryCtx,
				durationMs: 1,
			})

			expect(beforeQuery).not.toHaveBeenCalled()
			expect(afterQuery).not.toHaveBeenCalled()
		})
	})
})
