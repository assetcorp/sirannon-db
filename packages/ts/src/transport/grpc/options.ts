/**
 * Sets the address that the gRPC transport listens on and the certificates that it presents and trusts.
 *
 * @public
 */
export interface GrpcReplicationOptions {
  /**
   * Sets the address that the gRPC server binds to. Defaults to '0.0.0.0'.
   */
  host?: string
  /**
   * Sets the port that the gRPC server binds to. The default, 0, lets the operating system pick a free port.
   */
  port?: number
  /**
   * Sets the path to this node's TLS certificate.
   */
  tlsCert?: string
  /**
   * Sets the path to this node's private key.
   */
  tlsKey?: string
  /**
   * Sets the path to the certificate authority certificate that this node verifies its peers against.
   */
  tlsCaCert?: string
  /**
   * Turns off TLS. Use it only in tests.
   */
  insecure?: boolean
  /**
   * Sets how many milliseconds a replica waits for a forwarded write before it fails the write. Defaults to 30000.
   */
  forwardDeadlineMs?: number
  /**
   * Sets how many milliseconds each side of a replication stream waits for the other side's Hello. A client that waits
   * longer closes the connection and dials again, and a server ends the stream. Defaults to 10000.
   */
  helloTimeoutMs?: number
  /**
   * Sets how many milliseconds pass between the keepalive pings that this node sends on each connection that it dials,
   * so that it finds a connection that has gone silent. Defaults to 10000, and the shortest interval it accepts is 1000.
   */
  keepaliveIntervalMs?: number
  /**
   * Sets how many milliseconds this node waits for the answer to a keepalive ping before it treats the connection as
   * lost and dials again. Defaults to 10000.
   */
  keepaliveTimeoutMs?: number
}

export const DEFAULT_FORWARD_DEADLINE_MS = 30_000
export const SERVICE_NAME = 'sirannon.replication.v1.Replication'
