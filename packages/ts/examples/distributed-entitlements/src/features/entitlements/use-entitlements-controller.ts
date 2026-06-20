import type { RemoteSubscription } from '@delali/sirannon-db/client'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  applyBillingEvent,
  createCustomer,
  getClusterStatus,
  getControlPlaneSnapshot,
  healClusterLinks,
  isolateCurrentPrimary,
  recordUsage,
  replayDuplicateUsage,
  resetControlPlane,
} from '../../lib/app-actions.functions'
import type { CDCEvent } from '../../lib/cdc'
import { getMajorityWriteAvailability } from '../../lib/cluster-readiness'
import { subscribeControlPlane } from '../../lib/direct-client'
import type {
  ApplyBillingEventInput,
  ControlPlaneSnapshot,
  CreateCustomerInput,
  CustomerEntitlement,
  RecordUsageInput,
} from '../../lib/schemas'
import {
  createIdempotencyKey,
  formatEventLabel,
  getControlPlaneStats,
  selectedCustomerOrFirst,
  toErrorMessage,
} from './entitlements-utils'
import type { BillingDraft, ConnectionState, ControllerState, LoaderData, UsageDraft } from './types'

const LIVE_REFRESH_DELAY_MS = 160
const CLUSTER_READINESS_REFRESH_MS = 1_000

export function useEntitlementsController(initialData: LoaderData) {
  const [state, setState] = useState<ControllerState>({
    customers: initialData.customers,
    usage: initialData.usage,
    billingEvents: initialData.billingEvents,
    auditLog: initialData.auditLog,
    clusterNodes: initialData.clusterNodes,
  })
  const [selectedCustomerId, setSelectedCustomerId] = useState<number | null>(initialData.customers[0]?.id ?? null)
  const [connectionState, setConnectionState] = useState<ConnectionState>('connecting')
  const [pendingAction, setPendingAction] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [lastEvent, setLastEvent] = useState('Waiting for a replicated change')
  const [error, setError] = useState<string | null>(initialData.initialError)
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const snapshot = useMemo<ControlPlaneSnapshot>(
    () => ({
      customers: state.customers,
      usage: state.usage,
      billingEvents: state.billingEvents,
      auditLog: state.auditLog,
      clusterNodes: state.clusterNodes,
    }),
    [state],
  )
  const stats = useMemo(() => getControlPlaneStats(snapshot), [snapshot])
  const selectedCustomer = useMemo(
    () => selectedCustomerOrFirst(state.customers, selectedCustomerId),
    [selectedCustomerId, state.customers],
  )
  const writeAvailability = useMemo(() => getMajorityWriteAvailability(state.clusterNodes), [state.clusterNodes])
  const writeAvailable = writeAvailability.available
  const writeUnavailableReason = writeAvailability.reason

  const replaceSnapshot = useCallback((nextSnapshot: ControlPlaneSnapshot) => {
    setState({
      customers: nextSnapshot.customers,
      usage: nextSnapshot.usage,
      billingEvents: nextSnapshot.billingEvents,
      auditLog: nextSnapshot.auditLog,
      clusterNodes: nextSnapshot.clusterNodes,
    })
  }, [])

  const refreshSnapshot = useCallback(async () => {
    const nextSnapshot = await getControlPlaneSnapshot()
    replaceSnapshot(nextSnapshot)
  }, [replaceSnapshot])

  const refreshClusterStatus = useCallback(async () => {
    const clusterNodes = await getClusterStatus()
    setState(current => ({ ...current, clusterNodes }))
  }, [])

  const queueLiveRefresh = useCallback(() => {
    if (refreshTimerRef.current !== null) {
      return
    }

    refreshTimerRef.current = setTimeout(() => {
      refreshTimerRef.current = null
      refreshSnapshot()
        .then(() => {
          setError(null)
        })
        .catch(refreshError => {
          setError(toErrorMessage(refreshError))
        })
    }, LIVE_REFRESH_DELAY_MS)
  }, [refreshSnapshot])

  const handleLiveEvent = useCallback(
    (event: CDCEvent) => {
      setLastEvent(formatEventLabel(event))
      queueLiveRefresh()
    },
    [queueLiveRefresh],
  )

  useEffect(() => {
    let disposed = false
    let subscriptions: RemoteSubscription[] = []

    setConnectionState('connecting')

    subscribeControlPlane(handleLiveEvent)
      .then(nextSubscriptions => {
        if (disposed) {
          for (const subscription of nextSubscriptions) {
            subscription.unsubscribe()
          }
          return
        }
        subscriptions = nextSubscriptions
        setConnectionState('live')
      })
      .catch(subscriptionError => {
        if (disposed) {
          return
        }
        setConnectionState('offline')
        setError(toErrorMessage(subscriptionError))
      })

    return () => {
      disposed = true
      for (const subscription of subscriptions) {
        subscription.unsubscribe()
      }
    }
  }, [handleLiveEvent])

  useEffect(() => {
    const nextSelected = selectedCustomerOrFirst(state.customers, selectedCustomerId)
    if (nextSelected && nextSelected.id !== selectedCustomerId) {
      setSelectedCustomerId(nextSelected.id)
    }
  }, [selectedCustomerId, state.customers])

  useEffect(() => {
    return () => {
      if (refreshTimerRef.current !== null) {
        clearTimeout(refreshTimerRef.current)
      }
    }
  }, [])

  useEffect(() => {
    if (writeAvailable) return

    let disposed = false
    let timer: ReturnType<typeof setTimeout> | null = null
    const scheduleRefresh = () => {
      timer = setTimeout(() => {
        refreshClusterStatus()
          .catch(() => undefined)
          .finally(() => {
            if (!disposed) {
              scheduleRefresh()
            }
          })
      }, CLUSTER_READINESS_REFRESH_MS)
    }

    scheduleRefresh()
    return () => {
      disposed = true
      if (timer !== null) {
        clearTimeout(timer)
      }
    }
  }, [refreshClusterStatus, writeAvailable])

  const runMutation = useCallback(
    async (label: string, mutation: () => Promise<void>): Promise<boolean> => {
      setPendingAction(label)
      try {
        await mutation()
        await refreshSnapshot()
        setError(null)
        return true
      } catch (mutationError) {
        setError(toErrorMessage(mutationError))
        return false
      } finally {
        setPendingAction(null)
      }
    },
    [refreshSnapshot],
  )

  const runWriteMutation = useCallback(
    async (label: string, mutation: () => Promise<void>): Promise<boolean> => {
      if (!writeAvailable) {
        setError(`Write blocked: ${writeUnavailableReason}`)
        return false
      }
      return runMutation(label, mutation)
    },
    [runMutation, writeAvailable, writeUnavailableReason],
  )

  const handleRefreshClick = useCallback(() => {
    setRefreshing(true)
    refreshSnapshot()
      .then(() => {
        setError(null)
      })
      .catch(refreshError => {
        setError(toErrorMessage(refreshError))
      })
      .finally(() => {
        setRefreshing(false)
      })
  }, [refreshSnapshot])

  const handleResetClick = useCallback(async () => {
    await runWriteMutation('Resetting control plane', resetControlPlane)
  }, [runWriteMutation])

  const handleCreateCustomer = useCallback(
    async (input: CreateCustomerInput): Promise<boolean> => {
      return runWriteMutation(`Creating ${input.name}`, async () => {
        await createCustomer({ data: input })
      })
    },
    [runWriteMutation],
  )

  const handleSelectCustomer = useCallback((customer: CustomerEntitlement) => {
    setSelectedCustomerId(customer.id)
  }, [])

  const handleRecordUsage = useCallback(
    async (draft: UsageDraft): Promise<boolean> => {
      if (!selectedCustomer) {
        setError('Select a customer before recording usage')
        return false
      }

      const input: RecordUsageInput = {
        customerId: selectedCustomer.id,
        customerName: selectedCustomer.name,
        units: draft.units,
        source: draft.source,
        idempotencyKey: draft.idempotencyKey,
      }

      return runWriteMutation(`Recording ${draft.units} units`, async () => {
        await recordUsage({ data: input })
      })
    },
    [runWriteMutation, selectedCustomer],
  )

  const handleReplayDuplicateUsage = useCallback(async (): Promise<boolean> => {
    if (!selectedCustomer) {
      setError('Select a customer before replaying usage')
      return false
    }

    const input: RecordUsageInput = {
      customerId: selectedCustomer.id,
      customerName: selectedCustomer.name,
      units: 250,
      source: 'billing_replay',
      idempotencyKey: createIdempotencyKey('usage-replay', selectedCustomer.id),
    }

    return runWriteMutation('Replaying duplicate usage', async () => {
      await replayDuplicateUsage({ data: input })
    })
  }, [runWriteMutation, selectedCustomer])

  const handleApplyBilling = useCallback(
    async (draft: BillingDraft): Promise<boolean> => {
      if (!selectedCustomer) {
        setError('Select a customer before applying a billing event')
        return false
      }

      const input: ApplyBillingEventInput = {
        ...draft,
        customerExternalId: selectedCustomer.external_id,
        customerName: selectedCustomer.name,
      }

      return runWriteMutation(`Applying ${draft.eventType}`, async () => {
        await applyBillingEvent({ data: input })
      })
    },
    [runWriteMutation, selectedCustomer],
  )

  const handleIsolatePrimary = useCallback(async () => {
    await runMutation('Isolating current primary', isolateCurrentPrimary)
  }, [runMutation])

  const handleHealCluster = useCallback(async () => {
    await runMutation('Healing cluster links', healClusterLinks)
  }, [runMutation])

  const handleDismissError = useCallback(() => {
    setError(null)
  }, [])

  return {
    ...state,
    selectedCustomer,
    stats,
    connectionState,
    pendingAction,
    refreshing,
    lastEvent,
    error,
    writeAvailability,
    handleRefreshClick,
    handleResetClick,
    handleCreateCustomer,
    handleSelectCustomer,
    handleRecordUsage,
    handleReplayDuplicateUsage,
    handleApplyBilling,
    handleIsolatePrimary,
    handleHealCluster,
    handleDismissError,
  }
}
