import type {
  ClusterStatusInfo,
  NodeHealthReason,
  NodeHealthState,
  ReadConcernLevel,
  ServerOptions,
  SirannonOptions,
} from '../../../core/types.js'
import { type RequestTally, requestTally, type ServedShop, servedShop } from './harness.js'

export interface ClusterNode {
  readonly shop: ServedShop
  readonly tally: RequestTally
  readonly base: string
  readonly row: string
}

export type Routing = Omit<ClusterStatusInfo, 'databaseId'>

export interface RoutingHolder {
  current: Routing | null
  readonly getClusterStatus: (databaseId: string) => ClusterStatusInfo | null
}

export function routingHolder(): RoutingHolder {
  const holder: RoutingHolder = {
    current: null,
    getClusterStatus: databaseId => (holder.current === null ? null : { databaseId, ...holder.current }),
  }
  return holder
}

export async function clusterNode(
  row: string,
  server: ServerOptions = {},
  registry: Partial<SirannonOptions> = {},
): Promise<ClusterNode> {
  const tally = requestTally()
  const shop = await servedShop({
    label: row,
    customers: [row],
    registry,
    server: { authenticate: tally.authenticate, ...server },
  })
  return { shop, tally, base: shop.served.base, row }
}

export function coordinatedNode(
  row: string,
  holder: RoutingHolder,
  server: ServerOptions = {},
  registry: Partial<SirannonOptions> = {},
): Promise<ClusterNode> {
  return clusterNode(
    row,
    {
      getClusterStatus: holder.getClusterStatus,
      authorizeClusterStatus: () => true,
      ...server,
    },
    registry,
  )
}

export function readable(nodeId: string, endpoint: string, ...readConcerns: ReadConcernLevel[]) {
  return { nodeId, endpoint, readConcerns }
}

export function healthyRouting(primary: string | null, readEndpoints: ReturnType<typeof readable>[]): Routing {
  const health: NodeHealthState = primary === null ? 'unavailable' : 'healthy'
  const healthReason: NodeHealthReason = primary === null ? 'coordinator-unreachable' : 'in-sync'
  return {
    currentPrimary: primary === null ? null : { nodeId: 'primary', endpoint: primary },
    primaryTerm: 3n,
    readEndpoints,
    health,
    healthReason,
  }
}

export function dataRequests(node: ClusterNode, route: 'query' | 'execute' | 'transaction' | 'batch' | 'load'): number {
  return node.tally.requests.filter(ctx => ctx.path.startsWith(`/db/shop/${route}`)).length
}
