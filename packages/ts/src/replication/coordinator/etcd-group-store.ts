import type { Namespace, Watcher } from 'etcd3'
import { CoordinatorError, NoSafePrimaryError } from '../errors.js'
import { parseGroupState, parseNodeSession, serializeGroupState } from './etcd-codec.js'
import { nodeSessionKey, replicationGroupKey } from './etcd-connection.js'
import {
  assertNonEmpty,
  assertNonNegativeSeq,
  assertNonNegativeTerm,
  assertPrimaryInGroup,
  buildReplicationGroupState,
  cloneReplicationGroupState,
  isEligiblePromotionSession,
  MIN_AUTOMATIC_FAILOVER_VOTERS,
  markDisplacedPrimaryForRepair,
  nextAdmittedInSyncState,
  nextInSyncSetState,
  nextMaintenanceState,
  normaliseNodeIds,
} from './group-rules.js'
import type {
  AdmitNodeToInSyncSetInput,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
  CoordinatorNodeSession,
  CoordinatorWatchDisposer,
  PromoteEligibleReplicaInput,
  ReplicationGroupState,
  ReplicationGroupWatcher,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
} from './types.js'

const MAX_CAS_ATTEMPTS = 5
const MAX_PROMOTION_ATTEMPTS = 3

interface CasGroupStateResult {
  updated: boolean
  state: ReplicationGroupState | null
}

/**
 * The replication group half of the etcd coordinator. Every write is a
 * compare-and-swap against the value the read returned, so two controllers
 * racing on the same group leave one of them to retry against the winner's
 * state rather than overwriting it.
 */
export class EtcdGroupStore {
  constructor(
    private readonly namespace: Namespace,
    private readonly watchers: Set<Watcher>,
    private readonly onWatcherError: ((error: Error) => void) | undefined,
  ) {}

  async setReplicationGroupState(input: SetReplicationGroupStateInput): Promise<ReplicationGroupState> {
    const state = buildReplicationGroupState(input)
    await this.namespace.put(replicationGroupKey(input.clusterId, input.groupId)).value(serializeGroupState(state))
    return cloneReplicationGroupState(state)
  }

  async getReplicationGroupState(clusterId: string, groupId: string): Promise<ReplicationGroupState | null> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(groupId, 'groupId')
    const value = await this.namespace.get(replicationGroupKey(clusterId, groupId)).string()
    return value ? parseGroupState(value) : null
  }

  async watchReplicationGroup(
    clusterId: string,
    groupId: string,
    watcher: ReplicationGroupWatcher,
  ): Promise<CoordinatorWatchDisposer> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(groupId, 'groupId')
    const key = replicationGroupKey(clusterId, groupId)
    const etcdWatcher = await this.namespace.watch().key(key).create()
    this.watchers.add(etcdWatcher)
    etcdWatcher.on('put', kv => {
      try {
        watcher(parseGroupState(kv.value.toString('utf8')))
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.onWatcherError?.(wrappedErr)
      }
    })
    etcdWatcher.on('error', err => {
      this.onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
    })
    return async () => {
      this.watchers.delete(etcdWatcher)
      await etcdWatcher.cancel()
    }
  }

  async compareAndAdvancePrimaryTerm(
    input: CompareAndAdvancePrimaryTermInput,
  ): Promise<CompareAndAdvancePrimaryTermResult> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonNegativeTerm(input.expectedPrimaryTerm)
    assertNonEmpty(input.nextPrimary.nodeId, 'nextPrimary.nodeId')

    const key = replicationGroupKey(input.clusterId, input.groupId)
    const currentRaw = await this.namespace.get(key).string()
    if (!currentRaw) {
      return { advanced: false, state: null }
    }
    const current = parseGroupState(currentRaw)
    if (current.primaryTerm !== input.expectedPrimaryTerm) {
      return { advanced: false, state: current }
    }
    assertPrimaryInGroup(input.nextPrimary, current.votingDataBearingNodeIds)

    const next: ReplicationGroupState = {
      ...current,
      currentPrimary: { ...input.nextPrimary },
      primaryTerm: current.primaryTerm + 1n,
      updatedAtMs: Date.now(),
    }
    markDisplacedPrimaryForRepair(next, current.currentPrimary?.nodeId, input.nextPrimary.nodeId)
    const nextRaw = serializeGroupState(next)
    const result = await this.namespace
      .if(key, 'Value', '==', currentRaw)
      .then(this.namespace.put(key).value(nextRaw))
      .commit()
    if (result.succeeded) {
      return { advanced: true, state: cloneReplicationGroupState(next) }
    }

    return { advanced: false, state: await this.getReplicationGroupState(input.clusterId, input.groupId) }
  }

  async updateInSyncSet(input: UpdateInSyncSetInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    if (input.durabilityPointSeq !== undefined) {
      assertNonNegativeSeq(input.durabilityPointSeq, 'durabilityPointSeq')
    }
    normaliseNodeIds(input.inSyncNodeIds, 'inSyncNodeIds')

    return this.updateGroupStateWithRetry(input.clusterId, input.groupId, state =>
      nextInSyncSetState(state, input, Date.now()),
    )
  }

  async admitNodeToInSyncSet(input: AdmitNodeToInSyncSetInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonEmpty(input.nodeId, 'nodeId')
    assertNonEmpty(input.sourceNodeId, 'sourceNodeId')
    assertNonNegativeSeq(input.appliedSeq, 'appliedSeq')

    return this.updateGroupStateWithRetry(input.clusterId, input.groupId, state =>
      nextAdmittedInSyncState(state, input, Date.now()),
    )
  }

  async updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonEmpty(input.nodeId, 'nodeId')

    return this.updateGroupStateWithRetry(input.clusterId, input.groupId, state =>
      nextMaintenanceState(state, input, Date.now()),
    )
  }

  async promoteEligibleReplica(input: PromoteEligibleReplicaInput): Promise<ReplicationGroupState> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    const excludedNodeIds = new Set(input.excludeNodeIds ?? [])

    for (let attempt = 0; attempt < MAX_PROMOTION_ATTEMPTS; attempt++) {
      const state = await this.getReplicationGroupState(input.clusterId, input.groupId)
      if (!state) {
        throw new NoSafePrimaryError(`No replication group '${input.groupId}' is registered`)
      }
      if (state.votingDataBearingNodeIds.length < MIN_AUTOMATIC_FAILOVER_VOTERS) {
        throw new NoSafePrimaryError(
          `Automatic promotion requires at least ${MIN_AUTOMATIC_FAILOVER_VOTERS} voting data-bearing nodes`,
        )
      }

      for (const nodeId of state.votingDataBearingNodeIds) {
        if (nodeId === state.currentPrimary?.nodeId || excludedNodeIds.has(nodeId)) {
          continue
        }
        const session = await this.getLiveNodeSession(input.clusterId, nodeId)
        if (!isEligiblePromotionSession(state, nodeId, session)) {
          continue
        }
        const advanced = await this.compareAndAdvancePrimaryTerm({
          clusterId: input.clusterId,
          groupId: input.groupId,
          expectedPrimaryTerm: state.primaryTerm,
          nextPrimary: session.endpoint ? { nodeId, endpoint: session.endpoint } : { nodeId },
        })
        if (advanced.advanced && advanced.state) {
          return advanced.state
        }
      }
    }

    throw new NoSafePrimaryError(`No safe primary is available for replication group '${input.groupId}'`)
  }

  private async getLiveNodeSession(clusterId: string, nodeId: string): Promise<CoordinatorNodeSession | null> {
    const value = await this.namespace.get(nodeSessionKey(clusterId, nodeId)).string()
    return value ? parseNodeSession(value) : null
  }

  private async updateGroupStateWithRetry(
    clusterId: string,
    groupId: string,
    mutate: (state: ReplicationGroupState) => ReplicationGroupState,
  ): Promise<ReplicationGroupState | null> {
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const state = await this.getReplicationGroupState(clusterId, groupId)
      if (!state) {
        return null
      }
      const next = mutate(state)
      if (next === state) {
        return cloneReplicationGroupState(state)
      }
      const result = await this.casGroupState(state, next)
      if (result.updated) {
        return result.state
      }
    }
    throw new CoordinatorError(`Failed to update replication group '${groupId}' after concurrent coordinator writes`)
  }

  private async casGroupState(
    previous: ReplicationGroupState,
    next: ReplicationGroupState,
  ): Promise<CasGroupStateResult> {
    const key = replicationGroupKey(previous.clusterId, previous.groupId)
    const previousRaw = serializeGroupState(previous)
    const nextRaw = serializeGroupState(next)
    const result = await this.namespace
      .if(key, 'Value', '==', previousRaw)
      .then(this.namespace.put(key).value(nextRaw))
      .commit()
    if (result.succeeded) {
      return { updated: true, state: cloneReplicationGroupState(next) }
    }
    return { updated: false, state: await this.getReplicationGroupState(previous.clusterId, previous.groupId) }
  }
}
