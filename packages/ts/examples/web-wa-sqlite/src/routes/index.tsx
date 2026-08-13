import { createFileRoute } from '@tanstack/react-router'
import { FieldServiceApp } from '../features/field-service/field-service-app'

export const Route = createFileRoute('/')({
  validateSearch: (search: Record<string, unknown>): { device?: string } => {
    const device = search.device
    return typeof device === 'string' && device.length > 0 ? { device } : {}
  },
  component: IndexComponent,
})

function IndexComponent() {
  const { device } = Route.useSearch()
  return <FieldServiceApp deviceName={device} />
}
