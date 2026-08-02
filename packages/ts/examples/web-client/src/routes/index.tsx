import { createFileRoute } from '@tanstack/react-router'
import { InventoryDemo } from '../features/inventory/inventory-demo'

export const Route = createFileRoute('/')({
  component: InventoryRoute,
})

function InventoryRoute() {
  return <InventoryDemo />
}
