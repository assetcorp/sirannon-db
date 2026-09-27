import { Badge } from '@delali/sirannon-example-shared/ui/badge'
import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Separator } from '@delali/sirannon-example-shared/ui/separator'
import { ArrowRight, HardDrive } from 'lucide-react'
import { useCallback, useEffect, useState } from 'react'
import { listDevicesHeldElsewhere, listKnownDevices } from '../../../lib/device-registry'
import { DeviceNameForm } from './device-name-form'

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

      <DeviceNameForm label={knownDevices.length > 0 ? 'Or add a new device' : 'Device name'} onPick={onPick} />
    </div>
  )
}
