import { readFileSync } from 'node:fs'
import type { EtcdClusterCoordinator } from '../../replication/coordinator/etcd.js'
import { CoordinatorCallHold } from './coordinator-call-hold.js'
import { GroupReadHold } from './group-read-hold.js'
import type { FailoverNodeConfig } from './node-process.js'

type CreateEtcdCoordinator = typeof import('./cluster-node-library.js')['createEtcdCoordinator']

export const inSyncUpdateHold = new CoordinatorCallHold()
export const groupReadHold = new GroupReadHold()

export function createFailoverCoordinator(
  create: CreateEtcdCoordinator,
  config: FailoverNodeConfig,
): EtcdClusterCoordinator {
  const coordinator = connectEtcd(create, config)
  const sendInSyncUpdate = coordinator.updateInSyncSet.bind(coordinator)
  coordinator.updateInSyncSet = async input => {
    await inSyncUpdateHold.pass(input)
    return sendInSyncUpdate(input)
  }
  const sendAdmission = coordinator.admitNodeToInSyncSet.bind(coordinator)
  coordinator.admitNodeToInSyncSet = async input => {
    await inSyncUpdateHold.pass(input)
    return sendAdmission(input)
  }
  const readGroup = coordinator.getReplicationGroupState.bind(coordinator)
  coordinator.getReplicationGroupState = async (clusterId, groupId) => {
    const state = await readGroup(clusterId, groupId)
    await groupReadHold.pass()
    return state
  }
  if (config.dieAfterGroupCreate) {
    const createGroup = coordinator.createReplicationGroupState.bind(coordinator)
    coordinator.createReplicationGroupState = async input => {
      const result = await createGroup(input)
      process.kill(process.pid, 'SIGKILL')
      return result
    }
  }
  return coordinator
}

function connectEtcd(create: CreateEtcdCoordinator, config: FailoverNodeConfig): EtcdClusterCoordinator {
  const timeouts = { dialTimeoutMs: 1_000, defaultCallTimeoutMs: 1_000 }
  if (config.etcdAccess === 'insecure-test') {
    return create({
      hosts: config.etcdHosts,
      keyPrefix: config.keyPrefix,
      allowInsecure: true,
      ...timeouts,
    })
  }
  if (config.etcdAccess === 'plain-http') {
    return create({ hosts: config.etcdHosts, keyPrefix: config.keyPrefix, ...timeouts })
  }
  const httpsHosts = config.etcdHosts.map(host => host.replace(/^http:/, 'https:'))
  if (config.etcdAccess === 'https-without-credentials') {
    return create({ hosts: httpsHosts, keyPrefix: config.keyPrefix, ...timeouts })
  }
  return create({
    hosts: httpsHosts,
    keyPrefix: config.keyPrefix,
    credentials: { rootCertificate: readFileSync(config.caCertPath) },
    ...timeouts,
  })
}
