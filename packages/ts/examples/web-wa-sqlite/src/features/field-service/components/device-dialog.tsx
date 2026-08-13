import { Button } from '@delali/sirannon-example-shared/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@delali/sirannon-example-shared/ui/dialog'
import { useNavigate } from '@tanstack/react-router'
import { ChevronsUpDown, Smartphone } from 'lucide-react'
import { useCallback, useState } from 'react'

import { DevicePicker } from './device-picker'

export function DeviceDialog({ currentDevice }: { currentDevice: string }) {
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)

  const handlePick = useCallback(
    (name: string) => {
      setOpen(false)
      if (name !== currentDevice) {
        void navigate({ to: '/', search: { device: name } })
      }
    },
    [currentDevice, navigate],
  )

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <Smartphone data-icon="inline-start" aria-hidden="true" />
          <span className="max-w-32 truncate font-mono text-[13px]">{currentDevice}</span>
          <ChevronsUpDown data-icon="inline-end" aria-hidden="true" />
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Switch device</DialogTitle>
          <DialogDescription>
            This tab is <span className="font-mono">{currentDevice}</span>. Switching opens another device's local
            database; the one here stays in this browser and keeps its unsynced work.
          </DialogDescription>
        </DialogHeader>
        <DevicePicker currentDevice={currentDevice} onPick={handlePick} />
      </DialogContent>
    </Dialog>
  )
}
