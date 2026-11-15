import { TransportError } from '../../replication/errors.js'
import { DEFAULT_FORWARD_DEADLINE_MS, type GrpcReplicationOptions } from './options.js'

export const DEFAULT_HELLO_TIMEOUT_MS = 10_000
export const DEFAULT_KEEPALIVE_INTERVAL_MS = 10_000
export const DEFAULT_KEEPALIVE_TIMEOUT_MS = 10_000
export const SHORTEST_KEEPALIVE_INTERVAL_MS = 1_000
export const LONGEST_TIMER_MS = 2_147_483_647

const SHORTEST_WAIT_MS = 1

export interface ConnectionTiming {
  readonly forwardDeadlineMs: number
  readonly helloTimeoutMs: number
  readonly keepaliveIntervalMs: number
  readonly keepaliveTimeoutMs: number
}

function milliseconds(name: string, value: unknown, fallback: number, shortest: number): number {
  if (value === undefined) return fallback
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < shortest || value > LONGEST_TIMER_MS) {
    throw new TransportError(
      `${name} must be a whole number of milliseconds from ${shortest} to ${LONGEST_TIMER_MS}, and it was ${String(value)}`,
    )
  }
  return value
}

/**
 * Returns the deadlines and keepalive intervals that a gRPC transport uses, taking each one from the options or from
 * its default.
 *
 * @param options - The options that the caller passed to the transport.
 * @returns The forward deadline, the Hello timeout, the keepalive interval, and the keepalive timeout, in milliseconds.
 * @throws A `TRANSPORT_ERROR` when a value is not a whole number of milliseconds, is longer than a timer holds, or is
 * shorter than its floor, which is 1,000 for `keepaliveIntervalMs` and 1 for the others.
 *
 * @internal
 */
export function resolveConnectionTiming(options: GrpcReplicationOptions): ConnectionTiming {
  return {
    forwardDeadlineMs: milliseconds(
      'forwardDeadlineMs',
      options.forwardDeadlineMs,
      DEFAULT_FORWARD_DEADLINE_MS,
      SHORTEST_WAIT_MS,
    ),
    helloTimeoutMs: milliseconds('helloTimeoutMs', options.helloTimeoutMs, DEFAULT_HELLO_TIMEOUT_MS, SHORTEST_WAIT_MS),
    keepaliveIntervalMs: milliseconds(
      'keepaliveIntervalMs',
      options.keepaliveIntervalMs,
      DEFAULT_KEEPALIVE_INTERVAL_MS,
      SHORTEST_KEEPALIVE_INTERVAL_MS,
    ),
    keepaliveTimeoutMs: milliseconds(
      'keepaliveTimeoutMs',
      options.keepaliveTimeoutMs,
      DEFAULT_KEEPALIVE_TIMEOUT_MS,
      SHORTEST_WAIT_MS,
    ),
  }
}
