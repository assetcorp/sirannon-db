import { useCommand, useLiveQuery } from '@delali/sirannon-db/react'
import { useCallback, useMemo, useState } from 'react'
import { main } from '../../generated/operations'
import { addProduct, allocateProduct, receiveInventory, resetInventory } from '../../lib/app-actions.functions'
import { liveDatabase } from '../../lib/live-database'
import type { AddProductInput, Product } from '../../lib/schemas'
import { activityRecordSchema, productSchema } from '../../lib/schemas'
import {
  firstLiveError,
  getProductStats,
  isRevalidating,
  parseRows,
  toConnectionState,
  toErrorMessage,
} from './inventory-utils'
import type { WriteMode } from './types'
import { RECEIVE_QUANTITY } from './types'

const NO_ARGS: Record<string, never> = {}

export function useInventoryController() {
  const [mode, setMode] = useState<WriteMode>('app-server')
  const [pendingAction, setPendingAction] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)

  const productsState = useLiveQuery(liveDatabase, main.reads.products, NO_ARGS)
  const activityState = useLiveQuery(liveDatabase, main.reads.activity, NO_ARGS)

  const allocateFromBrowser = useCommand(liveDatabase, main.writes.allocateProduct)
  const receiveFromBrowser = useCommand(liveDatabase, main.writes.receiveInventory)
  const addFromBrowser = useCommand(liveDatabase, main.writes.addProduct)
  const resetFromBrowser = useCommand(liveDatabase, main.writes.resetInventory)

  const parsedProducts = useMemo(() => parseRows(productsState, productSchema), [productsState])
  const parsedActivity = useMemo(() => parseRows(activityState, activityRecordSchema), [activityState])

  const products = parsedProducts.rows
  const activity = parsedActivity.rows
  const stats = useMemo(() => getProductStats(products), [products])

  const liveStates = useMemo(() => [productsState, activityState], [productsState, activityState])
  const connectionState = toConnectionState(liveStates)
  const revalidating = isRevalidating(liveStates)
  const rejectedRows = parsedProducts.rejected + parsedActivity.rejected

  const error =
    actionError ??
    firstLiveError(liveStates) ??
    (rejectedRows > 0 ? `${rejectedRows} rows did not match the expected shape` : null)

  const runMutation = useCallback(async (label: string, mutation: () => Promise<unknown>): Promise<boolean> => {
    setPendingAction(label)
    try {
      await mutation()
      setActionError(null)
      return true
    } catch (mutationError) {
      setActionError(toErrorMessage(mutationError))
      return false
    } finally {
      setPendingAction(null)
    }
  }, [])

  const handleModeChange = useCallback((nextMode: WriteMode) => {
    setMode(nextMode)
    setActionError(null)
  }, [])

  const handleResetClick = useCallback(async () => {
    await runMutation('Resetting records', () =>
      mode === 'browser-direct' ? resetFromBrowser(NO_ARGS) : resetInventory(),
    )
  }, [mode, resetFromBrowser, runMutation])

  const handleAllocateProduct = useCallback(
    async (product: Product) => {
      await runMutation(`Allocating ${product.name}`, () =>
        mode === 'browser-direct'
          ? allocateFromBrowser({ productId: product.id })
          : allocateProduct({ data: { productId: product.id } }),
      )
    },
    [mode, allocateFromBrowser, runMutation],
  )

  const handleReceiveInventory = useCallback(
    async (product: Product) => {
      const args = { productId: product.id, quantity: RECEIVE_QUANTITY }
      await runMutation(`Receiving ${product.name}`, () =>
        mode === 'browser-direct' ? receiveFromBrowser(args) : receiveInventory({ data: args }),
      )
    },
    [mode, receiveFromBrowser, runMutation],
  )

  const handleAddProduct = useCallback(
    async (input: AddProductInput): Promise<boolean> => {
      return runMutation(`Creating ${input.name}`, () =>
        mode === 'browser-direct' ? addFromBrowser(input) : addProduct({ data: input }),
      )
    },
    [mode, addFromBrowser, runMutation],
  )

  const handleDismissError = useCallback(() => {
    setActionError(null)
  }, [])

  return {
    mode,
    products,
    activity,
    connectionState,
    revalidating,
    pendingAction,
    error,
    stats,
    handleModeChange,
    handleResetClick,
    handleAllocateProduct,
    handleReceiveInventory,
    handleAddProduct,
    handleDismissError,
  }
}
