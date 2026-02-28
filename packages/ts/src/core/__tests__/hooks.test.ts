import { describe, it, expect, vi } from 'vitest'
import { HookRegistry } from '../hooks/registry.js'
import type { HookConfig, QueryHookContext } from '../types.js'

describe('HookRegistry', () => {
	describe('register and invoke', () => {
		it('invokes a registered hook with the provided context', async () => {
			const registry = new HookRegistry()
			const hook = vi.fn()
			const ctx: QueryHookContext = {
				databaseId: 'main',
				sql: 'SELECT 1',
			}

			registry.register('beforeQuery', hook)
			await registry.invoke('beforeQuery', ctx)

			expect(hook).toHaveBeenCalledOnce()
			expect(hook).toHaveBeenCalledWith(ctx)
		})

		it('invokes multiple hooks for the same event in registration order', async () => {
			const registry = new HookRegistry()
			const order: number[] = []

			registry.register('beforeQuery', () => { order.push(1) })
			registry.register('beforeQuery', () => { order.push(2) })
			registry.register('beforeQuery', () => { order.push(3) })

			await registry.invoke('beforeQuery', {})

			expect(order).toEqual([1, 2, 3])
		})

		it('does nothing when invoking an event with no hooks', async () => {
			const registry = new HookRegistry()
			await expect(registry.invoke('beforeQuery', {})).resolves.toBeUndefined()
		})

		it('keeps hooks for different events independent', async () => {
			const registry = new HookRegistry()
			const beforeQuery = vi.fn()
			const afterQuery = vi.fn()

			registry.register('beforeQuery', beforeQuery)
			registry.register('afterQuery', afterQuery)

			await registry.invoke('beforeQuery', {})

			expect(beforeQuery).toHaveBeenCalledOnce()
			expect(afterQuery).not.toHaveBeenCalled()
		})
	})

	describe('async hooks', () => {
		it('awaits async hooks before proceeding to the next one', async () => {
			const registry = new HookRegistry()
			const order: number[] = []

			registry.register('beforeQuery', async () => {
				await new Promise((r) => setTimeout(r, 10))
				order.push(1)
			})
			registry.register('beforeQuery', () => {
				order.push(2)
			})

			await registry.invoke('beforeQuery', {})

			expect(order).toEqual([1, 2])
		})
	})

	describe('throw-to-deny', () => {
		it('propagates errors thrown by sync hooks', async () => {
			const registry = new HookRegistry()
			registry.register('beforeQuery', () => {
				throw new Error('access denied')
			})

			await expect(registry.invoke('beforeQuery', {})).rejects.toThrow(
				'access denied',
			)
		})

		it('propagates errors thrown by async hooks', async () => {
			const registry = new HookRegistry()
			registry.register('beforeQuery', async () => {
				throw new Error('async denial')
			})

			await expect(registry.invoke('beforeQuery', {})).rejects.toThrow(
				'async denial',
			)
		})

		it('stops invoking remaining hooks after one throws', async () => {
			const registry = new HookRegistry()
			const secondHook = vi.fn()

			registry.register('beforeQuery', () => {
				throw new Error('denied')
			})
			registry.register('beforeQuery', secondHook)

			await expect(registry.invoke('beforeQuery', {})).rejects.toThrow('denied')
			expect(secondHook).not.toHaveBeenCalled()
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

			await registry.invoke('beforeQuery', {})
			await registry.invoke('afterQuery', {})

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
			await registry.invoke('beforeQuery', {})

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

			await registry.invoke('beforeQuery', {})
			await registry.invoke('afterQuery', {})
			await registry.invoke('beforeConnect', {})
			await registry.invoke('databaseOpen', {})
			await registry.invoke('databaseClose', {})
			await registry.invoke('beforeSubscribe', {})

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
			await registry.invoke('beforeQuery', {})

			expect(hook).not.toHaveBeenCalled()
		})

		it('clears all hooks when called without arguments', async () => {
			const registry = new HookRegistry()
			const beforeQuery = vi.fn()
			const afterQuery = vi.fn()

			registry.register('beforeQuery', beforeQuery)
			registry.register('afterQuery', afterQuery)
			registry.clear()

			await registry.invoke('beforeQuery', {})
			await registry.invoke('afterQuery', {})

			expect(beforeQuery).not.toHaveBeenCalled()
			expect(afterQuery).not.toHaveBeenCalled()
		})
	})
})
