import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { generate } from 'selfsigned'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { GrpcReplicationTransport } from '../../index.js'
import { teardown, waitFor } from './_helpers.js'

describe('GrpcReplicationTransport', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    await teardown(transports)
  })

  describe('mTLS authentication', () => {
    let certDir: string
    let caCertPath: string
    let serverCertPath: string
    let serverKeyPath: string
    let clientCertPath: string
    let clientKeyPath: string
    let wrongCertPath: string
    let wrongKeyPath: string

    beforeAll(async () => {
      certDir = mkdtempSync(join(tmpdir(), 'sirannon-grpc-tls-'))
      const tomorrow = new Date(Date.now() + 86_400_000)

      const caAttrs = [{ name: 'commonName', value: 'Sirannon Test CA' }]
      const caResult = await generate(caAttrs, {
        keySize: 2048,
        notAfterDate: tomorrow,
        extensions: [
          { name: 'basicConstraints', cA: true },
          { name: 'keyUsage', keyCertSign: true, cRLSign: true },
        ],
      })

      caCertPath = join(certDir, 'ca.pem')
      writeFileSync(caCertPath, caResult.cert)

      const serverAttrs = [{ name: 'commonName', value: 'primary-node' }]
      const serverResult = await generate(serverAttrs, {
        keySize: 2048,
        notAfterDate: tomorrow,
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
      })

      serverCertPath = join(certDir, 'server.pem')
      serverKeyPath = join(certDir, 'server-key.pem')
      writeFileSync(serverCertPath, serverResult.cert)
      writeFileSync(serverKeyPath, serverResult.private)

      const clientAttrs = [{ name: 'commonName', value: 'replica-node' }]
      const clientResult = await generate(clientAttrs, {
        keySize: 2048,
        notAfterDate: tomorrow,
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
      })

      clientCertPath = join(certDir, 'client.pem')
      clientKeyPath = join(certDir, 'client-key.pem')
      writeFileSync(clientCertPath, clientResult.cert)
      writeFileSync(clientKeyPath, clientResult.private)

      const wrongResult = await generate([{ name: 'commonName', value: 'wrong-node' }], {
        keySize: 2048,
        notAfterDate: tomorrow,
      })
      wrongCertPath = join(certDir, 'wrong.pem')
      wrongKeyPath = join(certDir, 'wrong-key.pem')
      writeFileSync(wrongCertPath, wrongResult.cert)
      writeFileSync(wrongKeyPath, wrongResult.private)
    })

    afterAll(() => {
      try {
        rmSync(certDir, { recursive: true, force: true })
      } catch {
        /* best-effort cleanup */
      }
    })

    it('connects with valid mTLS certificates', async () => {
      const primary = new GrpcReplicationTransport({
        port: 0,
        tlsCert: serverCertPath,
        tlsKey: serverKeyPath,
        tlsCaCert: caCertPath,
      })
      transports.push(primary)

      await primary.connect('primary-node', { localRole: 'primary' })
      const port = primary.getPort()

      const replica = new GrpcReplicationTransport({
        tlsCert: clientCertPath,
        tlsKey: clientKeyPath,
        tlsCaCert: caCertPath,
      })
      transports.push(replica)

      await replica.connect('replica-node', {
        localRole: 'replica',
        endpoints: [`localhost:${port}`],
      })

      await waitFor(() => primary.peers().size > 0 && replica.peers().size > 0, 10_000)

      expect(primary.peers().has('replica-node')).toBe(true)
      expect(replica.peers().has('primary-node')).toBe(true)
    })

    it('rejects connection with wrong certificate', async () => {
      const primary = new GrpcReplicationTransport({
        port: 0,
        tlsCert: serverCertPath,
        tlsKey: serverKeyPath,
        tlsCaCert: caCertPath,
      })
      transports.push(primary)

      await primary.connect('primary-node', { localRole: 'primary' })
      const port = primary.getPort()

      const wrongReplica = new GrpcReplicationTransport({
        tlsCert: wrongCertPath,
        tlsKey: wrongKeyPath,
        tlsCaCert: caCertPath,
      })
      transports.push(wrongReplica)

      await wrongReplica.connect('wrong-replica', {
        localRole: 'replica',
        endpoints: [`localhost:${port}`],
      })

      await new Promise(resolve => setTimeout(resolve, 2000))

      expect(primary.peers().has('wrong-replica')).toBe(false)
    })
  })
})
