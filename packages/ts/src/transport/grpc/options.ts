/**
 * @public
 *
 * Where the gRPC transport listens, and the certificates it presents and trusts.
 */
export interface GrpcReplicationOptions {
  /**
   * Address the gRPC server binds to. Default: '0.0.0.0'.
   */
  host?: string
  /**
   * Port the gRPC server binds to. Pass 0 to take any free port.
   */
  port?: number
  /**
   * Path to this node's certificate.
   */
  tlsCert?: string
  /**
   * Path to this node's private key.
   */
  tlsKey?: string
  /**
   * Path to the authority certificate this node verifies its peers against.
   */
  tlsCaCert?: string
  /**
   * Runs without TLS, which suits tests only.
   */
  insecure?: boolean
  /**
   * Milliseconds a forwarded write may take before the replica gives up. Default: 30000.
   */
  forwardDeadlineMs?: number
}

export const DEFAULT_FORWARD_DEADLINE_MS = 30_000
export const SERVICE_NAME = 'sirannon.replication.v1.Replication'
