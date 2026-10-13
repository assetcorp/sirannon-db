import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { generate } from 'selfsigned'
import type { MtlsCerts, NodeCert } from './certs.js'

const DAY_MS = 86_400_000

interface GeneratedPair {
  cert: string
  private: string
}

export interface CertificateRequest {
  label: string
  commonName: string
  notBefore?: Date
  notAfter?: Date
}

export interface CertificateAuthority extends MtlsCerts {
  issued(label: string): NodeCert
}

export async function createCertificateAuthority(
  authorityName: string,
  requests: readonly CertificateRequest[],
): Promise<CertificateAuthority> {
  const certDir = mkdtempSync(join(tmpdir(), 'sirannon-e2e-authority-'))
  const authority = (await generate([{ name: 'commonName', value: authorityName }], {
    keySize: 2048,
    notAfterDate: new Date(Date.now() + DAY_MS),
    extensions: [
      { name: 'basicConstraints', cA: true },
      { name: 'keyUsage', keyCertSign: true, cRLSign: true },
    ],
  })) as GeneratedPair
  const caCertPath = join(certDir, 'ca.pem')
  writeFileSync(caCertPath, authority.cert)

  const issued = new Map<string, NodeCert>()
  for (const request of requests) {
    const pair = (await generate([{ name: 'commonName', value: request.commonName }], {
      keySize: 2048,
      ...(request.notBefore ? { notBeforeDate: request.notBefore } : {}),
      notAfterDate: request.notAfter ?? new Date(Date.now() + DAY_MS),
      extensions: [
        {
          name: 'subjectAltName',
          altNames: [
            { type: 2, value: 'localhost' },
            { type: 7, ip: '127.0.0.1' },
          ],
        },
      ],
      ca: { key: authority.private, cert: authority.cert },
    })) as GeneratedPair
    const certPath = join(certDir, `${request.label}.pem`)
    const keyPath = join(certDir, `${request.label}-key.pem`)
    writeFileSync(certPath, pair.cert)
    writeFileSync(keyPath, pair.private)
    issued.set(request.label, { certPath, keyPath })
  }

  const lookup = (label: string): NodeCert => {
    const cert = issued.get(label)
    if (cert === undefined) throw new Error(`No certificate issued under the label ${label}`)
    return cert
  }

  return {
    caCertPath,
    certDir,
    certForNode: lookup,
    issued: lookup,
    cleanup: () => rmSync(certDir, { recursive: true, force: true }),
  }
}

export function expiredWindow(): { notBefore: Date; notAfter: Date } {
  const now = Date.now()
  return { notBefore: new Date(now - 3 * DAY_MS), notAfter: new Date(now - DAY_MS) }
}
