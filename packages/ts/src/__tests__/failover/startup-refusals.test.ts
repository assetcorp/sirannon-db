import { describe, expect, it } from 'vitest'
import { type GateEnvironment, NODE_A, type NodeOptions, startNode } from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import type { SerializedError } from './node-process.js'

const FIRST_NODE: NodeOptions = { role: 'primary', seedSchema: true, createsGroup: true, dialTargets: [] }

async function startupFailureOf(env: GateEnvironment, options: NodeOptions): Promise<SerializedError> {
  const node = await startNode(env, NODE_A, { ...FIRST_NODE, ...options, awaitReady: false })
  const failure = await node.startupFailure()
  expect(node.running).toBe(false)
  return failure
}

describe('a node that refuses to start', () => {
  const environment = useFailoverEnvironment()

  it('refuses etcd access without TLS, without credentials, or with a CA alone, and writes nothing to etcd', async () => {
    const env = environment()
    const plainHttp = await startupFailureOf(env, { etcdAccess: 'plain-http' })
    expect(plainHttp.message).toMatch(/requires https etcd endpoints/)
    const withoutCredentials = await startupFailureOf(env, { etcdAccess: 'https-without-credentials' })
    expect(withoutCredentials.message).toMatch(/requires TLS credentials/)
    const caOnly = await startupFailureOf(env, { etcdAccess: 'https-with-ca-only' })
    expect(caOnly.message).toMatch(/requires an authenticated Sirannon identity/)
    expect(await env.etcdOracle.keysUnderPrefix()).toEqual([])
  })

  it('refuses coordinator mode without a stable node identifier, and without the etcd3 package installed', async () => {
    const env = environment()
    const withoutNodeId = await startupFailureOf(env, { stableNodeId: false })
    expect(withoutNodeId.code).toBe('AUTHORITY_LOST')
    expect(withoutNodeId.message).toMatch(/stable persisted nodeId/)
    expect(await env.etcdOracle.keysUnderPrefix()).toEqual([])

    const withoutEtcd3 = await startupFailureOf(env, { etcd3Unresolvable: true })
    expect(withoutEtcd3.code).toBe('COORDINATOR_DEPENDENCY_MISSING')
    expect(withoutEtcd3.message).toContain('pnpm add -E etcd3')
    expect(await env.etcdOracle.keysUnderPrefix()).toEqual([])
  })
})
