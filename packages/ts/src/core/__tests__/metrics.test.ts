import { describe, expect, it, vi } from 'vitest'
import { MetricsCollector } from '../metrics/collector.js'
import type { CDCMetrics, ConnectionMetrics, QueryMetrics } from '../types.js'

describe('MetricsCollector', () => {
  describe('trackQuery', () => {
    it('returns the result of the wrapped function', () => {
      const onQueryComplete = vi.fn()
      const collector = new MetricsCollector({
        onQueryComplete,
      })

      const result = collector.trackQuery(() => [{ id: 1, name: 'alice' }], {
        databaseId: 'main',
        sql: 'SELECT * FROM users',
      })

      expect(result).toEqual([{ id: 1, name: 'alice' }])
    })

    it('invokes onQueryComplete with timing data', () => {
      const onQueryComplete = vi.fn()
      const collector = new MetricsCollector({
        onQueryComplete,
      })

      collector.trackQuery(() => [], {
        databaseId: 'main',
        sql: 'SELECT 1',
        rowsReturned: 1,
      })

      expect(onQueryComplete).toHaveBeenCalledOnce()
      const metrics: QueryMetrics = onQueryComplete.mock.calls[0][0]
      expect(metrics.databaseId).toBe('main')
      expect(metrics.sql).toBe('SELECT 1')
      expect(metrics.rowsReturned).toBe(1)
      expect(typeof metrics.durationMs).toBe('number')
      expect(metrics.durationMs).toBeGreaterThanOrEqual(0)
    })

    it('measures duration with reasonable accuracy', () => {
      const onQueryComplete = vi.fn()
      const collector = new MetricsCollector({
        onQueryComplete,
      })

      collector.trackQuery(
        () => {
          const end = performance.now() + 20
          while (performance.now() < end) {
            /* spin */
          }
          return null
        },
        {
          databaseId: 'main',
          sql: 'SELECT pg_sleep(0.02)',
        },
      )

      const metrics: QueryMetrics = onQueryComplete.mock.calls[0][0]
      expect(metrics.durationMs).toBeGreaterThanOrEqual(15)
    })

    it('reports metrics when the wrapped function throws', () => {
      const onQueryComplete = vi.fn()
      const collector = new MetricsCollector({
        onQueryComplete,
      })

      expect(() =>
        collector.trackQuery(
          () => {
            throw new Error('query failed')
          },
          { databaseId: 'main', sql: 'INVALID SQL' },
        ),
      ).toThrow('query failed')

      expect(onQueryComplete).toHaveBeenCalledOnce()
      const metrics: QueryMetrics = onQueryComplete.mock.calls[0][0]
      expect(metrics.sql).toBe('INVALID SQL')
      expect(typeof metrics.durationMs).toBe('number')
    })

    it('sets error to false on success', () => {
      const onQueryComplete = vi.fn()
      const collector = new MetricsCollector({
        onQueryComplete,
      })

      collector.trackQuery(() => 'ok', {
        databaseId: 'main',
        sql: 'SELECT 1',
      })

      const metrics: QueryMetrics = onQueryComplete.mock.calls[0][0]
      expect(metrics.error).toBe(false)
    })

    it('sets error to true on failure', () => {
      const onQueryComplete = vi.fn()
      const collector = new MetricsCollector({
        onQueryComplete,
      })

      expect(() =>
        collector.trackQuery(
          () => {
            throw new Error('boom')
          },
          { databaseId: 'main', sql: 'BAD SQL' },
        ),
      ).toThrow('boom')

      const metrics: QueryMetrics = onQueryComplete.mock.calls[0][0]
      expect(metrics.error).toBe(true)
    })

    it('skips timing overhead when no callback is configured', () => {
      const collector = new MetricsCollector()

      const result = collector.trackQuery(() => 42, {
        databaseId: 'main',
        sql: 'SELECT 42',
      })

      expect(result).toBe(42)
    })

    it('passes changes and rowsReturned through', () => {
      const onQueryComplete = vi.fn()
      const collector = new MetricsCollector({
        onQueryComplete,
      })

      collector.trackQuery(() => null, {
        databaseId: 'main',
        sql: 'UPDATE users SET name = ?',
        changes: 5,
      })

      const metrics: QueryMetrics = onQueryComplete.mock.calls[0][0]
      expect(metrics.changes).toBe(5)
    })

    it('swallows callback errors on success and still returns the result', () => {
      const collector = new MetricsCollector({
        onQueryComplete: () => {
          throw new Error('callback exploded')
        },
      })

      const result = collector.trackQuery(() => [{ id: 1 }], {
        databaseId: 'main',
        sql: 'SELECT * FROM users',
      })

      expect(result).toEqual([{ id: 1 }])
    })

    it('swallows callback errors on failure and preserves the original error', () => {
      const collector = new MetricsCollector({
        onQueryComplete: () => {
          throw new Error('callback exploded')
        },
      })

      expect(() =>
        collector.trackQuery(
          () => {
            throw new Error('real query error')
          },
          { databaseId: 'main', sql: 'BAD SQL' },
        ),
      ).toThrow('real query error')
    })
  })

  describe('trackConnection', () => {
    it('invokes onConnectionOpen for open events', () => {
      const onConnectionOpen = vi.fn()
      const collector = new MetricsCollector({
        onConnectionOpen,
      })
      const metrics: ConnectionMetrics = {
        databaseId: 'main',
        path: '/data/main.db',
        readerCount: 4,
        event: 'open',
      }

      collector.trackConnection(metrics)

      expect(onConnectionOpen).toHaveBeenCalledOnce()
      expect(onConnectionOpen).toHaveBeenCalledWith(metrics)
    })

    it('invokes onConnectionClose for close events', () => {
      const onConnectionClose = vi.fn()
      const collector = new MetricsCollector({
        onConnectionClose,
      })
      const metrics: ConnectionMetrics = {
        databaseId: 'main',
        path: '/data/main.db',
        readerCount: 0,
        event: 'close',
      }

      collector.trackConnection(metrics)

      expect(onConnectionClose).toHaveBeenCalledOnce()
      expect(onConnectionClose).toHaveBeenCalledWith(metrics)
    })

    it('does nothing when no callback is configured', () => {
      const collector = new MetricsCollector()
      const metrics: ConnectionMetrics = {
        databaseId: 'main',
        path: '/data/main.db',
        readerCount: 4,
        event: 'open',
      }

      expect(() => collector.trackConnection(metrics)).not.toThrow()
    })

    it('swallows callback errors', () => {
      const collector = new MetricsCollector({
        onConnectionOpen: () => {
          throw new Error('callback exploded')
        },
      })

      expect(() =>
        collector.trackConnection({
          databaseId: 'main',
          path: '/data/main.db',
          readerCount: 4,
          event: 'open',
        }),
      ).not.toThrow()
    })
  })

  describe('trackCDCEvent', () => {
    it('invokes the onCDCEvent callback', () => {
      const onCDCEvent = vi.fn()
      const collector = new MetricsCollector({ onCDCEvent })
      const metrics: CDCMetrics = {
        databaseId: 'main',
        table: 'users',
        operation: 'insert',
        subscriberCount: 3,
      }

      collector.trackCDCEvent(metrics)

      expect(onCDCEvent).toHaveBeenCalledOnce()
      expect(onCDCEvent).toHaveBeenCalledWith(metrics)
    })

    it('does nothing when no callback is configured', () => {
      const collector = new MetricsCollector()

      expect(() =>
        collector.trackCDCEvent({
          databaseId: 'main',
          table: 'users',
          operation: 'delete',
          subscriberCount: 0,
        }),
      ).not.toThrow()
    })

    it('swallows callback errors', () => {
      const collector = new MetricsCollector({
        onCDCEvent: () => {
          throw new Error('callback exploded')
        },
      })

      expect(() =>
        collector.trackCDCEvent({
          databaseId: 'main',
          table: 'users',
          operation: 'insert',
          subscriberCount: 1,
        }),
      ).not.toThrow()
    })
  })

  describe('active', () => {
    it('returns false when no callbacks are configured', () => {
      const collector = new MetricsCollector()
      expect(collector.active).toBe(false)
    })

    it('returns false for an empty config', () => {
      const collector = new MetricsCollector({})
      expect(collector.active).toBe(false)
    })

    it('returns true when onQueryComplete is configured', () => {
      const collector = new MetricsCollector({
        onQueryComplete: vi.fn(),
      })
      expect(collector.active).toBe(true)
    })

    it('returns true when onConnectionOpen is configured', () => {
      const collector = new MetricsCollector({
        onConnectionOpen: vi.fn(),
      })
      expect(collector.active).toBe(true)
    })

    it('returns true when onConnectionClose is configured', () => {
      const collector = new MetricsCollector({
        onConnectionClose: vi.fn(),
      })
      expect(collector.active).toBe(true)
    })

    it('returns true when onCDCEvent is configured', () => {
      const collector = new MetricsCollector({
        onCDCEvent: vi.fn(),
      })
      expect(collector.active).toBe(true)
    })
  })
})
