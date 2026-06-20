import { createFileRoute } from '@tanstack/react-router'
import { InventoryDemo } from '../features/inventory/inventory-demo'
import { toErrorMessage } from '../features/inventory/inventory-utils'
import { EMPTY_SNAPSHOT, type LoaderData } from '../features/inventory/types'
import { getInventorySnapshot } from '../lib/app-actions.functions'

async function loadInventory(): Promise<LoaderData> {
  try {
    const snapshot = await getInventorySnapshot()
    return { ...snapshot, initialError: null }
  } catch (error) {
    return {
      ...EMPTY_SNAPSHOT,
      initialError: toErrorMessage(error),
    }
  }
}

export const Route = createFileRoute('/')({
  component: InventoryRoute,
  loader: loadInventory,
})

function InventoryRoute() {
  const initialData = Route.useLoaderData()
  return <InventoryDemo initialData={initialData} />
}
