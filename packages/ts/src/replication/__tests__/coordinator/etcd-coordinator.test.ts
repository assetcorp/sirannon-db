import { describe, expect, it } from 'vitest'
import { createEtcdCoordinator } from '../../coordinator/etcd.js'

describe('EtcdClusterCoordinator', () => {
  it('requires TLS credentials for production coordinator access', () => {
    expect(() =>
      createEtcdCoordinator({
        hosts: 'https://127.0.0.1:2379',
        keyPrefix: 'sirannon/cluster-a',
      }),
    ).toThrow(/TLS credentials/)
  })

  it('requires explicit insecure opt-in for non-TLS endpoints', () => {
    expect(() =>
      createEtcdCoordinator({
        hosts: 'http://127.0.0.1:2379',
        keyPrefix: 'sirannon/cluster-a',
      }),
    ).toThrow(/https etcd endpoints/)
  })

  it('requires an authenticated production identity, not only a trusted CA', () => {
    expect(() =>
      createEtcdCoordinator({
        hosts: 'https://127.0.0.1:2379',
        keyPrefix: 'sirannon/cluster-a',
        credentials: {
          rootCertificate: Buffer.from('ca'),
        },
      }),
    ).toThrow(/authenticated Sirannon identity/)
  })
})
