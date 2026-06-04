import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { generate } from 'selfsigned'

interface CertResult {
  cert: string
  private: string
}

const CERT_VALIDITY_MS = 7 * 24 * 60 * 60 * 1000
const NODE_IDS = ['node-a', 'node-b', 'node-c'] as const

const certDir = process.env.CERT_DIR ?? join(process.cwd(), '.certs')
const expiry = new Date(Date.now() + CERT_VALIDITY_MS)

await mkdir(certDir, { recursive: true })

const caResult = (await generate([{ name: 'commonName', value: 'Sirannon Entitlements Local CA' }], {
  keySize: 2048,
  notAfterDate: expiry,
  extensions: [
    { name: 'basicConstraints', cA: true },
    { name: 'keyUsage', keyCertSign: true, cRLSign: true },
  ],
})) as CertResult

await writeFile(join(certDir, 'ca.pem'), caResult.cert)

for (const nodeId of NODE_IDS) {
  const nodeResult = (await generate([{ name: 'commonName', value: nodeId }], {
    keySize: 2048,
    notAfterDate: expiry,
    extensions: [
      {
        name: 'subjectAltName',
        altNames: [
          { type: 2, value: nodeId },
          { type: 2, value: 'localhost' },
          { type: 2, value: 'toxiproxy' },
          { type: 7, ip: '127.0.0.1' },
        ],
      },
    ],
    ca: { key: caResult.private, cert: caResult.cert },
  })) as CertResult

  await writeFile(join(certDir, `${nodeId}.pem`), nodeResult.cert)
  await writeFile(join(certDir, `${nodeId}-key.pem`), nodeResult.private)
}

console.log(`Generated local mTLS certificates in ${certDir}`)
