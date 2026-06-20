import { ChevronRight, Database, type LucideIcon, Waves } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import { getMajorityWriteAvailability } from '../../../lib/cluster-readiness'
import type { ClusterNode } from '../../../lib/schemas'
import { clusterHealthLabel } from '../entitlements-utils'
import { StatusDot, type StatusTone, TONE_BADGE } from './status'

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

const HEALTH_TONE: Record<string, StatusTone> = {
  healthy: 'success',
  degraded: 'warning',
  failing_over: 'warning',
  repairing: 'warning',
  syncing: 'warning',
  unavailable: 'destructive',
  offline: 'destructive',
}

const TRANSITIONAL_HEALTH = new Set(['failing_over', 'repairing', 'syncing'])

export function ClusterBoard({ nodes }: { nodes: ClusterNode[] }) {
  const visibleNodes = nodes.length > 0 ? nodes : FALLBACK_NODES
  const primaryNode = visibleNodes.find(node => node.currentPrimary !== null)?.currentPrimary ?? 'unknown'
  const writeAvailability = getMajorityWriteAvailability(visibleNodes)
  const quorumTone: StatusTone = writeAvailability.available ? 'success' : 'warning'
  const tiles = visibleNodes.map(node => <NodeTile key={node.nodeId} node={node} primaryNode={primaryNode} />)

  return (
    <Card>
      <CardHeader>
        <CardTitle>Cluster topology</CardTitle>
        <CardDescription>Write path, replication fan-out, and change capture</CardDescription>
        <CardAction>
          <Badge variant="outline" className={cn('font-mono text-xs tabular-nums', TONE_BADGE[quorumTone])}>
            {writeAvailability.available
              ? 'majority ready'
              : `${writeAvailability.healthyVoters}/${writeAvailability.requiredVoters} majority`}
          </Badge>
        </CardAction>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-stretch">
          <PathStage icon={Database} label="Client" value="App server" />
          <FlowArrow />
          <div className="grid flex-1 gap-2 sm:grid-cols-3">{tiles}</div>
          <FlowArrow />
          <PathStage icon={Waves} label="Subscriber" value="CDC stream" />
        </div>
      </CardContent>
    </Card>
  )
}

function PathStage({ icon: Icon, label, value }: { icon: LucideIcon; label: string; value: string }) {
  return (
    <div className="border-border/80 bg-background/40 flex items-center gap-2.5 rounded-lg border border-dashed px-3 py-2.5 lg:w-44">
      <Icon className="text-muted-foreground size-4 shrink-0" aria-hidden="true" />
      <div className="flex min-w-0 flex-col">
        <span className="truncate text-sm font-medium">{value}</span>
        <span className="text-muted-foreground text-[10px] tracking-wider uppercase">{label}</span>
      </div>
    </div>
  )
}

function FlowArrow() {
  return (
    <ChevronRight className="text-muted-foreground/50 hidden size-4 shrink-0 self-center lg:block" aria-hidden="true" />
  )
}

function NodeTile({ node, primaryNode }: { node: ClusterNode; primaryNode: string }) {
  const health = clusterHealthLabel(node)
  const tone = HEALTH_TONE[health] ?? 'neutral'
  const isPrimary = primaryNode === node.nodeId

  return (
    <div
      className={cn(
        'bg-background/40 rounded-lg border p-3 transition-colors',
        tone === 'warning' && 'border-warning/40',
        tone === 'destructive' && 'border-destructive/40 opacity-80',
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="truncate font-mono text-sm font-medium">{node.nodeId}</span>
        <Badge
          variant="outline"
          className={cn('font-mono text-[10px] uppercase', isPrimary ? TONE_BADGE.success : 'text-muted-foreground')}
        >
          {isPrimary ? 'primary' : 'replica'}
        </Badge>
      </div>
      <div className="mt-2 flex items-center gap-2 text-xs">
        <StatusDot tone={tone} pulse={TRANSITIONAL_HEALTH.has(health)} />
        <span className="capitalize">{health.replace(/_/g, ' ')}</span>
      </div>
      <p className="text-muted-foreground mt-1 truncate font-mono text-[11px]">{node.endpoint}</p>
    </div>
  )
}
