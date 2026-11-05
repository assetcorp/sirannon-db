import type { GrpcReplicationOptions } from './options.js'

/**
 * Returns the common name in the certificate that a peer presented on a gRPC call, which names the peer's node ID when
 * the transport verifies certificates.
 *
 * @param options - The transport's options, which say whether it verifies certificates.
 * @param call - The call whose authentication context holds the peer's certificate.
 * @returns The first common name, or null when the transport verifies no certificate or the call carries none.
 *
 * @internal
 */
export function peerCertificateCommonName(
  options: GrpcReplicationOptions,
  call: { getAuthContext(): unknown },
): string | null {
  if (options.insecure) return null
  if (!options.tlsCaCert) return null

  const authCtx = call.getAuthContext() as { sslPeerCertificate?: { subject?: { CN?: string | string[] } } } | null
  if (!authCtx) return null
  const peerCert = authCtx.sslPeerCertificate
  if (!peerCert) return null
  const certCN = peerCert.subject?.CN
  if (!certCN) return null

  if (Array.isArray(certCN)) return certCN[0] ?? null
  return certCN
}
