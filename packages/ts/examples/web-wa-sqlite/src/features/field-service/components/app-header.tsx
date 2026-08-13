import { ThemeToggle } from '@delali/sirannon-example-shared/components/theme-toggle'
import { readStoredChoice } from '@delali/sirannon-example-shared/theme'
import { Label } from '@delali/sirannon-example-shared/ui/label'
import { Separator } from '@delali/sirannon-example-shared/ui/separator'
import { Switch } from '@delali/sirannon-example-shared/ui/switch'
import { Wrench } from 'lucide-react'
import { useId, useState } from 'react'

import { DeviceDialog } from './device-dialog'

export function AppHeader({
  deviceName,
  wantsOnline,
  onSyncEnabledChange,
}: {
  deviceName: string
  wantsOnline: boolean
  onSyncEnabledChange: (enabled: boolean) => void
}) {
  const syncSwitchId = useId()
  const [initialTheme] = useState(readStoredChoice)

  return (
    <header className="border-border/60 bg-background/80 sticky top-0 z-20 border-b backdrop-blur">
      <div className="mx-auto flex h-14 w-full max-w-6xl items-center gap-3 px-4 sm:px-6">
        <div className="bg-primary text-primary-foreground flex size-7 shrink-0 items-center justify-center rounded-md">
          <Wrench className="size-4" aria-hidden="true" />
        </div>
        <div className="min-w-0">
          <p className="truncate text-sm leading-tight font-bold">Field Service</p>
          <p className="text-muted-foreground truncate text-xs leading-tight">Sirannon device sync</p>
        </div>

        <div className="ml-auto flex items-center gap-2 sm:gap-3">
          <DeviceDialog currentDevice={deviceName} />
          <Separator orientation="vertical" className="hidden h-6 sm:block" />
          <div className="flex items-center gap-2">
            <Switch id={syncSwitchId} checked={wantsOnline} onCheckedChange={onSyncEnabledChange} />
            <Label htmlFor={syncSwitchId} className="hidden text-xs sm:block">
              Sync
            </Label>
          </div>
          <ThemeToggle initialChoice={initialTheme} />
        </div>
      </div>
    </header>
  )
}
