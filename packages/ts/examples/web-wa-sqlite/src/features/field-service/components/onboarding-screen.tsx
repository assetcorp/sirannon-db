import { Alert, AlertDescription, AlertTitle } from '@delali/sirannon-example-shared/ui/alert'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@delali/sirannon-example-shared/ui/card'
import { useNavigate } from '@tanstack/react-router'
import { TriangleAlert, Wrench } from 'lucide-react'
import { useCallback } from 'react'

import { DevicePicker } from './device-picker'

export function OnboardingScreen({ rejectedName }: { rejectedName?: string }) {
  const navigate = useNavigate()

  const handlePick = useCallback(
    (name: string) => {
      void navigate({ to: '/', search: { device: name } })
    },
    [navigate],
  )

  return (
    <main className="flex min-h-dvh items-center justify-center px-4 py-10">
      <div className="w-full max-w-md space-y-4">
        <div className="animate-rise flex items-center gap-3">
          <div className="bg-primary text-primary-foreground flex size-10 items-center justify-center rounded-lg shadow-sm">
            <Wrench className="size-5" aria-hidden="true" />
          </div>
          <div>
            <h1 className="text-lg leading-tight font-bold">Field Service</h1>
            <p className="text-muted-foreground text-sm">A Sirannon device sync example</p>
          </div>
        </div>

        {rejectedName !== undefined ? (
          <Alert variant="destructive" className="animate-rise">
            <TriangleAlert aria-hidden="true" />
            <AlertTitle>‘{rejectedName}’ is not a usable device name</AlertTitle>
            <AlertDescription>Pick one of the devices below or enter a valid name.</AlertDescription>
          </Alert>
        ) : null}

        <Card className="animate-rise">
          <CardHeader>
            <CardTitle>Name this device</CardTitle>
            <CardDescription>
              Each device keeps its own SQLite database in this browser and syncs it with the server. Open the same page
              in another tab under a different name and you have a second device.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <DevicePicker onPick={handlePick} />
          </CardContent>
        </Card>
      </div>
    </main>
  )
}
