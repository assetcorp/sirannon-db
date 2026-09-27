import { Alert, AlertDescription, AlertTitle } from '@delali/sirannon-example-shared/ui/alert'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@delali/sirannon-example-shared/ui/card'
import { TriangleAlert, Wrench } from 'lucide-react'
import { browserOnly } from '../../../lib/app-mode'
import { DeviceNameForm } from './device-name-form'
import { DevicePicker } from './device-picker'

export function OnboardingScreen({ rejectedName, onPick }: { rejectedName?: string; onPick: (name: string) => void }) {
  return (
    <main className="flex min-h-dvh items-center justify-center px-4 py-10">
      <div className="w-full max-w-md space-y-4">
        <div className="animate-rise flex items-center gap-3">
          <div className="bg-primary text-primary-foreground flex size-10 items-center justify-center rounded-lg shadow-sm">
            <Wrench className="size-5" aria-hidden="true" />
          </div>
          <div>
            <h1 className="text-lg leading-tight font-bold">Field Service</h1>
            <p className="text-muted-foreground text-sm">
              {browserOnly ? 'A Sirannon example on SQLite in the browser' : 'A Sirannon device sync example'}
            </p>
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
              {browserOnly
                ? 'This browser stores the work orders for this device in a SQLite database. Every order that you claim shows this name.'
                : 'Each device has its own SQLite database in this browser, which the app syncs with the server. When you open this page in a second tab under a different name, that tab becomes a second device.'}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {browserOnly ? <DeviceNameForm label="Device name" onPick={onPick} /> : <DevicePicker onPick={onPick} />}
          </CardContent>
        </Card>
      </div>
    </main>
  )
}
