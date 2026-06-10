import { Database, RadioTower, Server, Unplug, Waves } from 'lucide-react'
import type { ClusterNode } from '../../../lib/schemas'
import { cn } from '../../../lib/ui'
import { clusterHealthLabel } from '../entitlements-utils'
import { Badge } from './ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card'

const FALLBACK_NODES: ClusterNode[] = [
  {
    nodeId: 'node-a',
    endpoint: 'http://127.0.0.1:7301',
    reachable: false,
    currentPrimary: null,
    primaryTerm: null,
    readEndpoints: 0,
    health: 'unavailable',
    error: null,
  },
  {
    nodeId: 'node-b',
    endpoint: 'http://127.0.0.1:7302',
    reachable: false,
    currentPrimary: null,
    primaryTerm: null,
    readEndpoints: 0,
    health: 'unavailable',
    error: null,
  },
  {
    nodeId: 'node-c',
    endpoint: 'http://127.0.0.1:7303',
    reachable: false,
    currentPrimary: null,
    primaryTerm: null,
    readEndpoints: 0,
    health: 'unavailable',
    error: null,
  },
]

export function ClusterBoard({ nodes }: { nodes: ClusterNode[] }) {
  const visibleNodes = nodes.length > 0 ? nodes : FALLBACK_NODES
  const primaryNode = visibleNodes.find(node => node.currentPrimary !== null)?.currentPrimary ?? 'unknown'
  const availableNodes = visibleNodes.filter(node => node.reachable && node.health !== 'unavailable').length
  const cards = visibleNodes.map(node => <ClusterNodePill key={node.nodeId} node={node} primaryNode={primaryNode} />)

  return (
    <Card className="cluster-panel">
      <CardHeader>
        <div>
          <CardTitle>Cluster Path</CardTitle>
          <CardDescription>client, authority, replicas, subscriber</CardDescription>
        </div>
        <Badge variant={availableNodes >= 2 ? 'success' : 'warning'}>{availableNodes}/3 online</Badge>
      </CardHeader>
      <CardContent>
        <div className="cluster-path">
          <PathStage icon={<Database size={16} />} label="client" value="app server" />
          <PathStage icon={<RadioTower size={16} />} label="primary" value={primaryNode} active />
          <div className="node-strip">{cards}</div>
          <PathStage icon={<Waves size={16} />} label="live" value="CDC stream" />
        </div>
      </CardContent>
    </Card>
  )
}

function PathStage({
  icon,
  label,
  value,
  active = false,
}: {
  icon: React.ReactNode
  label: string
  value: string
  active?: boolean
}) {
  return (
    <div className={cn('path-stage', active && 'active')}>
      <span>{icon}</span>
      <div>
        <strong>{value}</strong>
        <em>{label}</em>
      </div>
    </div>
  )
}

function ClusterNodePill({ node, primaryNode }: { node: ClusterNode; primaryNode: string }) {
  const health = clusterHealthLabel(node)
  const isPrimary = primaryNode === node.nodeId
  const icon = node.reachable ? <Server size={15} /> : <Unplug size={15} />

  return (
    <div className={cn('node-pill', health, isPrimary && 'primary')}>
      <span>{icon}</span>
      <strong>{node.nodeId}</strong>
      <em>{health}</em>
    </div>
  )
}
