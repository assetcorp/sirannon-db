import type { ClusterNode } from './schemas'

export interface MajorityWriteAvailability {
  available: boolean
  healthyVoters: number
  requiredVoters: number
  reason: string
}

interface RoutingGroup {
  primaryNodeId: string
  primaryTerm: string | null
  reports: number
}

export function getMajorityWriteAvailability(nodes: ClusterNode[]): MajorityWriteAvailability {
  const requiredVoters = Math.floor(nodes.length / 2) + 1
  if (nodes.length === 0) {
    return {
      available: false,
      healthyVoters: 0,
      requiredVoters,
      reason: 'Cluster status is unavailable',
    }
  }

  const routingGroups = new Map<string, RoutingGroup>()
  for (const node of nodes) {
    if (!node.reachable || node.currentPrimary === null) continue
    const key = JSON.stringify([node.currentPrimary, node.primaryTerm])
    const existing = routingGroups.get(key)
    if (existing) {
      existing.reports += 1
      continue
    }
    routingGroups.set(key, {
      primaryNodeId: node.currentPrimary,
      primaryTerm: node.primaryTerm,
      reports: 1,
    })
  }

  let routing: RoutingGroup | null = null
  for (const candidate of routingGroups.values()) {
    if (routing === null || candidate.reports > routing.reports) {
      routing = candidate
    }
  }
  if (!routing || routing.reports < requiredVoters) {
    return {
      available: false,
      healthyVoters: 0,
      requiredVoters,
      reason: 'Cluster routing has no majority agreement',
    }
  }

  const matchesRouting = (node: ClusterNode) =>
    node.reachable && node.currentPrimary === routing.primaryNodeId && node.primaryTerm === routing.primaryTerm

  const primary = nodes.find(node => node.nodeId === routing.primaryNodeId && matchesRouting(node))
  const healthyVoters = nodes.filter(node => matchesRouting(node) && node.health === 'healthy').length

  if (!primary || primary.health !== 'healthy') {
    const primaryState = primary?.health ?? 'unreachable'
    return {
      available: false,
      healthyVoters,
      requiredVoters,
      reason: `Primary ${routing.primaryNodeId} is ${primaryState.replace(/_/g, ' ')}`,
    }
  }

  if (healthyVoters < requiredVoters) {
    return {
      available: false,
      healthyVoters,
      requiredVoters,
      reason: `Waiting for majority: ${healthyVoters}/${requiredVoters} healthy voters`,
    }
  }

  return {
    available: true,
    healthyVoters,
    requiredVoters,
    reason: `Majority available: ${healthyVoters}/${nodes.length} healthy voters`,
  }
}
