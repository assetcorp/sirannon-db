import { Crown, Radio, Server, Unplug } from 'lucide-react'
import type { ClusterNode } from '../../../lib/schemas'
import { clusterHealthLabel } from '../entitlements-utils'

export function ClusterBoard({ nodes }: { nodes: ClusterNode[] }) {
  const cards = nodes.map(node => <ClusterNodeCard key={node.nodeId} node={node} />)

  return <div className="cluster-board">{cards}</div>
}

function ClusterNodeCard({ node }: { node: ClusterNode }) {
  const health = clusterHealthLabel(node)
  const isPrimary = node.currentPrimary === node.nodeId

  return (
    <div className={`cluster-node ${health}`}>
      <div className="cluster-node-head">
        <span className="node-icon">{node.reachable ? <Server size={17} /> : <Unplug size={17} />}</span>
        <div>
          <strong>{node.nodeId}</strong>
          <span>{node.endpoint}</span>
        </div>
      </div>
      <div className="cluster-node-grid">
        <StatusPair label="Role" value={node.role ?? 'unknown'} />
        <StatusPair label="Health" value={health} />
        <StatusPair label="Reads" value={String(node.readEndpoints)} />
        <StatusPair label="Term" value={node.primaryTerm ?? 'none'} />
      </div>
      <div className="cluster-node-foot">
        <span className={isPrimary ? 'authority active' : 'authority'}>
          {isPrimary ? <Crown size={14} /> : <Radio size={14} />}
          {isPrimary ? 'primary authority' : `primary ${node.currentPrimary ?? 'unknown'}`}
        </span>
      </div>
      {node.error ? <p className="node-error">{node.error}</p> : null}
    </div>
  )
}

function StatusPair({ label, value }: { label: string; value: string }) {
  return (
    <div className="status-pair">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}
