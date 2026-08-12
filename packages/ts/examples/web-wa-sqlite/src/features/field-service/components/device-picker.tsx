import { Badge } from '@delali/sirannon-example-shared/ui/badge'
import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Input } from '@delali/sirannon-example-shared/ui/input'
import { Label } from '@delali/sirannon-example-shared/ui/label'
import { Separator } from '@delali/sirannon-example-shared/ui/separator'
import { ArrowRight, HardDrive, Plus } from 'lucide-react'
import { type ChangeEvent, type FormEvent, useCallback, useEffect, useId, useState } from 'react'
import {
  DEVICE_NAME_RULE,
  isValidDeviceName,
  listDevicesHeldElsewhere,
  listKnownDevices,
  normaliseDeviceName,
} from '../../../lib/device-registry'

function DeviceRow({
  name,
  current,
  heldElsewhere,
  onPick,
}: {
  name: string
  current: boolean
  heldElsewhere: boolean
  onPick: (name: string) => void
}) {
  const handleClick = useCallback(() => {
    onPick(name)
  }, [name, onPick])

  return (
    <Button
      variant="outline"
      className="w-full justify-start gap-2"
      disabled={current}
      onClick={handleClick}
      data-device={name}
    >
      <HardDrive className="text-muted-foreground size-4" aria-hidden="true" />
      <span className="font-mono text-[13px]">{name}</span>
      {current ? <Badge variant="secondary">this tab</Badge> : null}
      {heldElsewhere && !current ? <Badge variant="secondary">open in another tab</Badge> : null}
      <ArrowRight className="text-muted-foreground ml-auto size-3.5" aria-hidden="true" />
    </Button>
  )
}

export function DevicePicker({ currentDevice, onPick }: { currentDevice?: string; onPick: (name: string) => void }) {
  const inputId = useId()
  const [draft, setDraft] = useState('')
  const [invalid, setInvalid] = useState(false)
  const [knownDevices, setKnownDevices] = useState<string[]>([])
  const [heldElsewhere, setHeldElsewhere] = useState<ReadonlySet<string>>(new Set())

  useEffect(() => {
    setKnownDevices(listKnownDevices())
    let cancelled = false
    void listDevicesHeldElsewhere().then(held => {
      if (!cancelled) {
        setHeldElsewhere(held)
      }
    })
    return () => {
      cancelled = true
    }
  }, [])

  const handleDraftChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setDraft(event.target.value)
    setInvalid(false)
  }, [])

  const handleCreate = useCallback(
    (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      const name = normaliseDeviceName(draft)
      if (!isValidDeviceName(name)) {
        setInvalid(true)
        return
      }
      onPick(name)
    },
    [draft, onPick],
  )

  return (
    <div className="space-y-4">
      {knownDevices.length > 0 ? (
        <div className="space-y-2">
          <p className="text-muted-foreground text-xs font-medium tracking-wide uppercase">Devices in this browser</p>
          {knownDevices.map(name => (
            <DeviceRow
              key={name}
              name={name}
              current={name === currentDevice}
              heldElsewhere={heldElsewhere.has(name)}
              onPick={onPick}
            />
          ))}
          <Separator className="my-3" />
        </div>
      ) : null}

      <form className="space-y-2" onSubmit={handleCreate}>
        <Label htmlFor={inputId}>{knownDevices.length > 0 ? 'Or add a new device' : 'Device name'}</Label>
        <div className="flex gap-2">
          <Input
            id={inputId}
            value={draft}
            onChange={handleDraftChange}
            placeholder="van-1"
            autoComplete="off"
            spellCheck={false}
            className="font-mono"
            aria-invalid={invalid}
          />
          <Button type="submit">
            <Plus data-icon="inline-start" aria-hidden="true" />
            Open
          </Button>
        </div>
        <p className={invalid ? 'text-destructive text-xs' : 'text-muted-foreground text-xs'}>{DEVICE_NAME_RULE}</p>
      </form>
    </div>
  )
}
