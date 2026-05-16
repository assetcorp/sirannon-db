import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { generate } from 'selfsigned'

export interface NodeCert {
  certPath: string
  keyPath: string
}

export interface MtlsCerts {
  caCertPath: string
  certDir: string
  certForNode(nodeId: string): NodeCert
  cleanup(): void
}

interface CertResult {
  cert: string
  private: string
}

const CERT_VALIDITY_MS = 86_400_000

export async function createMtlsCerts(nodeIds: readonly string[]): Promise<MtlsCerts> {
  if (nodeIds.length === 0) {
    throw new Error('createMtlsCerts requires at least one node id')
  }

  const certDir = mkdtempSync(join(tmpdir(), 'sirannon-e2e-tls-'))
  const expiry = new Date(Date.now() + CERT_VALIDITY_MS)

  const caResult = (await generate([{ name: 'commonName', value: 'Sirannon E2E Test CA' }], {
    keySize: 2048,
    notAfterDate: expiry,
    extensions: [
      { name: 'basicConstraints', cA: true },
      { name: 'keyUsage', keyCertSign: true, cRLSign: true },
    ],
  })) as CertResult

  const caCertPath = join(certDir, 'ca.pem')
  writeFileSync(caCertPath, caResult.cert)

  const nodeCerts = new Map<string, NodeCert>()

  for (const nodeId of nodeIds) {
    const nodeResult = (await generate([{ name: 'commonName', value: nodeId }], {
      keySize: 2048,
      notAfterDate: expiry,
      extensions: [
        {
          name: 'subjectAltName',
          altNames: [
            { type: 2, value: 'localhost' },
            { type: 7, ip: '127.0.0.1' },
          ],
        },
      ],
      ca: { key: caResult.private, cert: caResult.cert },
    })) as CertResult

    const certPath = join(certDir, `${nodeId}.pem`)
    const keyPath = join(certDir, `${nodeId}-key.pem`)
    writeFileSync(certPath, nodeResult.cert)
    writeFileSync(keyPath, nodeResult.private)

    nodeCerts.set(nodeId, { certPath, keyPath })
  }

  return {
    caCertPath,
    certDir,
    certForNode(nodeId: string): NodeCert {
      const cert = nodeCerts.get(nodeId)
      if (!cert) {
        throw new Error(`No certificate generated for node id: ${nodeId}`)
      }
      return cert
    },
    cleanup(): void {
      try {
        rmSync(certDir, { recursive: true, force: true })
      } catch {
        /* best-effort cleanup */
      }
    },
  }
}
