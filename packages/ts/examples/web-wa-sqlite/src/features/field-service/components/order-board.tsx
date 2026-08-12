import { useLiveQuery } from '@delali/sirannon-db/react'
import { cn } from '@delali/sirannon-example-shared/lib/utils'
import { Alert, AlertDescription, AlertTitle } from '@delali/sirannon-example-shared/ui/alert'
import { Badge } from '@delali/sirannon-example-shared/ui/badge'
import { Skeleton } from '@delali/sirannon-example-shared/ui/skeleton'
import { TriangleAlert } from 'lucide-react'
import type { ReactNode } from 'react'
import type { FieldDevice } from '../../../lib/field-device'
import { WORK_ORDERS_QUERY } from '../../../lib/field-device'
import type { WorkOrder, WorkOrderStatus } from '../../../schema'
import { NewOrderForm } from './new-order-form'
import { OrderCard } from './order-card'

const LANES: readonly { status: WorkOrderStatus; title: string; dot: string }[] = [
  { status: 'scheduled', title: 'Scheduled', dot: 'bg-muted-foreground' },
  { status: 'in_progress', title: 'In progress', dot: 'bg-warning' },
  { status: 'done', title: 'Done', dot: 'bg-success' },
]

function Lane({ title, dot, count, children }: { title: string; dot: string; count: number; children: ReactNode }) {
  return (
    <section className="min-w-0">
      <header className="mb-3 flex items-center gap-2">
        <span className={cn('size-2 rounded-full', dot)} aria-hidden="true" />
        <h2 className="text-sm font-semibold tracking-tight">{title}</h2>
        <Badge variant="secondary" className="font-mono tabular-nums">
          {count}
        </Badge>
      </header>
      <div className="space-y-3">{children}</div>
    </section>
  )
}

function BoardSkeleton() {
  return (
    <div className="mt-6 grid gap-5 md:grid-cols-3">
      {LANES.map(lane => (
        <div key={lane.status} className="space-y-3">
          <Skeleton className="h-5 w-28" />
          <Skeleton className="h-28 w-full" />
          <Skeleton className="h-28 w-full" />
        </div>
      ))}
    </div>
  )
}

export function OrderBoard({
  device,
  onClaim,
  onComplete,
  onReopen,
  onCreate,
}: {
  device: FieldDevice
  onClaim: (id: string) => void
  onComplete: (id: string, note: string) => void
  onReopen: (id: string) => void
  onCreate: (site: string, task: string) => void
}) {
  const orders = useLiveQuery<WorkOrder>(device.liveDb, WORK_ORDERS_QUERY)

  if (orders.status === 'pending') {
    return <BoardSkeleton />
  }

  if (orders.status === 'error') {
    return (
      <Alert variant="destructive" className="mt-6">
        <TriangleAlert aria-hidden="true" />
        <AlertTitle>The work order list is unavailable</AlertTitle>
        <AlertDescription>{orders.error.message}</AlertDescription>
      </Alert>
    )
  }

  return (
    <div className="mt-6 grid gap-5 md:grid-cols-3">
      {LANES.map(lane => {
        const laneOrders = orders.rows.filter(order => order.status === lane.status)
        return (
          <Lane key={lane.status} title={lane.title} dot={lane.dot} count={laneOrders.length}>
            {lane.status === 'scheduled' ? <NewOrderForm onCreate={onCreate} /> : null}
            {laneOrders.map(order => (
              <OrderCard
                key={order.id}
                order={order}
                deviceName={device.name}
                onClaim={onClaim}
                onComplete={onComplete}
                onReopen={onReopen}
              />
            ))}
            {laneOrders.length === 0 && lane.status !== 'scheduled' ? (
              <p className="border-border text-muted-foreground rounded-lg border border-dashed px-3 py-6 text-center text-sm">
                Nothing {lane.title.toLowerCase()} yet.
              </p>
            ) : null}
          </Lane>
        )
      })}
    </div>
  )
}
