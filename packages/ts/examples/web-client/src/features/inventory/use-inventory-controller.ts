import { useCallback, useEffect, useMemo, useState } from 'react'
import type { CDCEvent } from '../../lib/cdc'
import { subscribeActivity, subscribeProducts } from '../../lib/direct-client'
import type { ActivityRecord, AddProductInput, InventorySnapshot, Product } from '../../lib/schemas'
import {
  allocateProductForMode,
  createProductForMode,
  fetchSnapshotForMode,
  receiveInventoryForMode,
  resetRecordsForMode,
} from './inventory-operations'
import {
  applyActivityEvent,
  applyProductEvent,
  createEmptyDispose,
  formatEventLabel,
  getProductStats,
  toErrorMessage,
} from './inventory-utils'
import type { ConnectionState, DemoMode, LoaderData } from './types'

export function useInventoryController(initialData: LoaderData) {
  const [mode, setMode] = useState<DemoMode>('app-actions')
  const [products, setProducts] = useState<Product[]>(initialData.products)
  const [activity, setActivity] = useState<ActivityRecord[]>(initialData.activity)
  const [connectionState, setConnectionState] = useState<ConnectionState>('connecting')
  const [pendingAction, setPendingAction] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [lastEvent, setLastEvent] = useState<string>('Waiting for a live change')
  const [error, setError] = useState<string | null>(initialData.initialError)

  const stats = useMemo(() => getProductStats(products), [products])

  const handleProductEvent = useCallback((event: CDCEvent) => {
    setProducts(currentProducts => applyProductEvent(currentProducts, event))
    setLastEvent(formatEventLabel(event))
  }, [])

  const handleActivityEvent = useCallback((event: CDCEvent) => {
    setActivity(currentActivity => applyActivityEvent(currentActivity, event))
    setLastEvent(formatEventLabel(event))
  }, [])

  useEffect(() => {
    let disposed = false
    let disposeProducts = createEmptyDispose()
    let disposeActivity = createEmptyDispose()

    setConnectionState('connecting')

    Promise.allSettled([subscribeProducts(handleProductEvent), subscribeActivity(handleActivityEvent)]).then(
      results => {
        const [productsResult, activityResult] = results
        const failedResult = results.find((result): result is PromiseRejectedResult => result.status === 'rejected')

        if (productsResult.status === 'fulfilled') {
          disposeProducts = productsResult.value.unsubscribe
        }

        if (activityResult.status === 'fulfilled') {
          disposeActivity = activityResult.value.unsubscribe
        }

        if (disposed || failedResult) {
          disposeProducts()
          disposeActivity()
          disposeProducts = createEmptyDispose()
          disposeActivity = createEmptyDispose()
        }

        if (disposed) {
          return
        }

        if (failedResult) {
          setConnectionState('offline')
          setError(toErrorMessage(failedResult.reason))
          return
        }

        setConnectionState('live')
      },
    )

    return () => {
      disposed = true
      disposeProducts()
      disposeActivity()
    }
  }, [handleActivityEvent, handleProductEvent])

  const replaceSnapshot = useCallback((snapshot: InventorySnapshot) => {
    setProducts(snapshot.products)
    setActivity(snapshot.activity)
  }, [])

  const refreshSnapshot = useCallback(async () => {
    const snapshot = await fetchSnapshotForMode(mode)
    replaceSnapshot(snapshot)
  }, [mode, replaceSnapshot])

  const runMutation = useCallback(async (label: string, mutation: () => Promise<void>): Promise<boolean> => {
    setPendingAction(label)
    try {
      await mutation()
      setError(null)
      return true
    } catch (mutationError) {
      setError(toErrorMessage(mutationError))
      return false
    } finally {
      setPendingAction(null)
    }
  }, [])

  const handleModeChange = useCallback((nextMode: DemoMode) => {
    setMode(nextMode)
    setError(null)
  }, [])

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
    await runMutation('Resetting records', async () => {
      await resetRecordsForMode(mode)
    })
  }, [mode, runMutation])

  const handleAllocateProduct = useCallback(
    async (product: Product) => {
      await runMutation(`Allocating ${product.name}`, async () => {
        await allocateProductForMode(mode, product)
      })
    },
    [mode, runMutation],
  )

  const handleReceiveInventory = useCallback(
    async (product: Product) => {
      await runMutation(`Receiving ${product.name}`, async () => {
        await receiveInventoryForMode(mode, product)
      })
    },
    [mode, runMutation],
  )

  const handleAddProduct = useCallback(
    async (input: AddProductInput): Promise<boolean> => {
      return runMutation(`Creating ${input.name}`, async () => {
        await createProductForMode(mode, input)
      })
    },
    [mode, runMutation],
  )

  const handleDismissError = useCallback(() => {
    setError(null)
  }, [])

  return {
    mode,
    products,
    activity,
    connectionState,
    pendingAction,
    refreshing,
    lastEvent,
    error,
    stats,
    handleModeChange,
    handleRefreshClick,
    handleResetClick,
    handleAllocateProduct,
    handleReceiveInventory,
    handleAddProduct,
    handleDismissError,
  }
}
