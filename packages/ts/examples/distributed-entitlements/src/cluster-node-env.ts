import type { TopologyRole } from '@delali/sirannon-db/replication'

export const NODE_IDS = ['node-a', 'node-b', 'node-c'] as const

export function requireNodeId(): (typeof NODE_IDS)[number] {
  const value = requireEnv('NODE_ID')
  if (NODE_IDS.includes(value as (typeof NODE_IDS)[number])) {
    return value as (typeof NODE_IDS)[number]
  }
  throw new Error(`NODE_ID must be one of ${NODE_IDS.join(', ')}`)
}

export function requireRole(): TopologyRole {
  const value = requireEnv('INITIAL_ROLE')
  if (value === 'primary' || value === 'replica') return value
  throw new Error('INITIAL_ROLE must be primary or replica')
}

export function requireClusterToken(): string {
  const value = requireEnv('SIRANNON_CLUSTER_TOKEN')
  console.log('Using SIRANNON_CLUSTER_TOKEN from the environment for cluster HTTP and WebSocket auth')
  return value
}

export function requireEnv(name: string): string {
  const value = process.env[name]
  if (!value) {
    throw new Error(`${name} is required`)
  }
  return value
}

export function requireCsv(name: string): string[] {
  const values = requireEnv(name)
    .split(',')
    .map(value => value.trim())
    .filter(value => value.length > 0)
  if (values.length === 0) {
    throw new Error(`${name} must contain at least one value`)
  }
  return values
}

export function requirePort(name: string): number {
  const port = numberEnv(name, Number.NaN)
  if (!Number.isSafeInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`${name} must be a valid TCP port`)
  }
  return port
}

export function numberEnv(name: string, fallback: number): number {
  const value = process.env[name]
  if (value === undefined) return fallback
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) {
    throw new Error(`${name} must be a finite number`)
  }
  return parsed
}
