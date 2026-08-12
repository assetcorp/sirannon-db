import { Alert, AlertDescription, AlertTitle } from '@delali/sirannon-example-shared/ui/alert'
import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@delali/sirannon-example-shared/ui/card'
import { useNavigate } from '@tanstack/react-router'
import { Lock, RotateCw } from 'lucide-react'
import { useCallback } from 'react'

import { DevicePicker } from './device-picker'

export function LockedScreen({ name }: { name: string }) {
  const navigate = useNavigate()

  const handlePick = useCallback(
    (picked: string) => {
      void navigate({ to: '/', search: { device: picked } })
    },
    [navigate],
  )

  const handleRetry = useCallback(() => {
    window.location.reload()
  }, [])

  return (
    <main className="flex min-h-dvh items-center justify-center px-4 py-10">
      <div className="w-full max-w-md space-y-4">
        <Alert className="animate-rise">
          <Lock aria-hidden="true" />
          <AlertTitle>
            Device <span className="font-mono">{name}</span> is open in another tab
          </AlertTitle>
          <AlertDescription>
            Two tabs on one device would share a single local database, which looks like sync but is only one file in
            two windows. Close the other tab, or open this tab as a different device.
          </AlertDescription>
        </Alert>

        <Card className="animate-rise">
          <CardHeader>
            <CardTitle>Use another device here</CardTitle>
            <CardDescription>Every device below is a separate SQLite database in this browser.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <DevicePicker currentDevice={name} onPick={handlePick} />
            <Button variant="ghost" className="w-full" onClick={handleRetry}>
              <RotateCw data-icon="inline-start" aria-hidden="true" />I closed the other tab, try again
            </Button>
          </CardContent>
        </Card>
      </div>
    </main>
  )
}
