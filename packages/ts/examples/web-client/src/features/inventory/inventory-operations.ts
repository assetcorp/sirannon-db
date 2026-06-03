import {
  addProduct,
  allocateProduct,
  getInventorySnapshot,
  receiveInventory,
  resetDatabase,
} from '../../lib/app-actions.functions'
import {
  addProductDirect,
  allocateProductDirect,
  fetchDirectSnapshot,
  receiveInventoryDirect,
  resetDatabaseDirect,
} from '../../lib/direct-client'
import type { AddProductInput, InventorySnapshot, Product, ReceiveInventoryInput } from '../../lib/schemas'
import type { DemoMode } from './types'
import { RECEIVE_QUANTITY } from './types'

export async function fetchSnapshotForMode(mode: DemoMode): Promise<InventorySnapshot> {
  if (mode === 'driver-access') {
    return fetchDirectSnapshot()
  }

  return getInventorySnapshot()
}

export async function resetRecordsForMode(mode: DemoMode): Promise<void> {
  if (mode === 'driver-access') {
    await resetDatabaseDirect()
    return
  }

  await resetDatabase()
}

export async function allocateProductForMode(mode: DemoMode, product: Product): Promise<void> {
  const input = { id: product.id, name: product.name }
  if (mode === 'driver-access') {
    await allocateProductDirect(input)
    return
  }

  await allocateProduct({ data: input })
}

export async function receiveInventoryForMode(mode: DemoMode, product: Product): Promise<void> {
  const input: ReceiveInventoryInput = {
    id: product.id,
    name: product.name,
    quantity: RECEIVE_QUANTITY,
  }

  if (mode === 'driver-access') {
    await receiveInventoryDirect(input)
    return
  }

  await receiveInventory({ data: input })
}

export async function createProductForMode(mode: DemoMode, input: AddProductInput): Promise<void> {
  if (mode === 'driver-access') {
    await addProductDirect(input)
    return
  }

  await addProduct({ data: input })
}
