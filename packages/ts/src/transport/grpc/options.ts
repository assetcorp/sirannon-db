export interface GrpcReplicationOptions {
  host?: string
  port?: number
  tlsCert?: string
  tlsKey?: string
  tlsCaCert?: string
  insecure?: boolean
  forwardDeadlineMs?: number
}

export const DEFAULT_FORWARD_DEADLINE_MS = 30_000
export const SERVICE_NAME = 'sirannon.replication.v1.Replication'
