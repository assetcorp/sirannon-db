import { Badge } from '@delali/sirannon-example-shared/ui/badge'
import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Card, CardContent } from '@delali/sirannon-example-shared/ui/card'
import { Input } from '@delali/sirannon-example-shared/ui/input'
import { CircleCheck, RotateCcw, UserRound } from 'lucide-react'
import { type ChangeEvent, type FormEvent, useCallback, useState } from 'react'

import type { WorkOrder } from '../../../schema'

function formatUpdatedAt(iso: string): string {
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) {
    return iso
  }
  return date.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

export function OrderCard({
  order,
  deviceName,
  onClaim,
  onComplete,
  onReopen,
}: {
  order: WorkOrder
  deviceName: string
  onClaim: (id: string) => void
  onComplete: (id: string, note: string) => void
  onReopen: (id: string) => void
}) {
  const [note, setNote] = useState('')

  const handleClaim = useCallback(() => {
    onClaim(order.id)
  }, [onClaim, order.id])

  const handleNoteChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setNote(event.target.value)
  }, [])

  const handleComplete = useCallback(
    (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      onComplete(order.id, note.trim())
      setNote('')
    },
    [note, onComplete, order.id],
  )

  const handleReopen = useCallback(() => {
    onReopen(order.id)
  }, [onReopen, order.id])

  const mine = order.technician === deviceName

  return (
    <Card className="animate-rise gap-0 py-4">
      <CardContent className="space-y-3 px-4">
        <div className="space-y-0.5">
          <p className="text-sm leading-snug font-semibold">{order.site}</p>
          <p className="text-muted-foreground text-sm leading-snug">{order.task}</p>
        </div>

        {order.technician.length > 0 ? (
          <Badge variant={mine ? 'default' : 'secondary'} className="gap-1">
            <UserRound className="size-3" aria-hidden="true" />
            <span className="font-mono">{order.technician}</span>
            {mine ? ' · you' : null}
          </Badge>
        ) : null}

        {order.status === 'done' && order.note.length > 0 ? (
          <p className="border-border text-muted-foreground border-l-2 pl-2 text-sm italic">{order.note}</p>
        ) : null}

        {order.status === 'scheduled' ? (
          <Button size="sm" onClick={handleClaim} data-order-action="claim">
            Claim
          </Button>
        ) : null}

        {order.status === 'in_progress' ? (
          <form className="flex gap-2" onSubmit={handleComplete}>
            <Input
              value={note}
              onChange={handleNoteChange}
              placeholder="Closing note"
              className="h-7 flex-1 text-sm"
              autoComplete="off"
            />
            <Button type="submit" size="sm" variant="secondary" data-order-action="complete">
              <CircleCheck data-icon="inline-start" aria-hidden="true" />
              Done
            </Button>
          </form>
        ) : null}

        {order.status === 'done' ? (
          <Button size="xs" variant="ghost" onClick={handleReopen} data-order-action="reopen">
            <RotateCcw data-icon="inline-start" aria-hidden="true" />
            Reopen
          </Button>
        ) : null}

        <p className="text-muted-foreground/80 font-mono text-[11px]">
          {order.id} · updated {formatUpdatedAt(order.updated_at)}
        </p>
      </CardContent>
    </Card>
  )
}
