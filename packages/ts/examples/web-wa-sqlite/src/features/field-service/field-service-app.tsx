import { Alert, AlertAction, AlertDescription, AlertTitle } from '@delali/sirannon-example-shared/ui/alert'
import { Button } from '@delali/sirannon-example-shared/ui/button'
import { useNavigate } from '@tanstack/react-router'
import { LoaderCircle, TriangleAlert, X } from 'lucide-react'
import { useCallback } from 'react'
import { isValidDeviceName, normaliseDeviceName } from '../../lib/device-registry'
import { claimWorkOrder, completeWorkOrder, createWorkOrder, reopenWorkOrder } from '../../lib/field-device'
import { AppHeader } from './components/app-header'
import { LockedScreen } from './components/locked-screen'
import { OnboardingScreen } from './components/onboarding-screen'
import { OrderBoard } from './components/order-board'
import { SnapshotPanel } from './components/snapshot-panel'
import { SyncStatusBar } from './components/sync-status-bar'
import { useFieldDevice } from './use-field-device'

export function FieldServiceApp({ deviceName }: { deviceName: string | undefined }) {
  if (deviceName === undefined) {
    return <OnboardingScreen />
  }
  const name = normaliseDeviceName(deviceName)
  if (!isValidDeviceName(name)) {
    return <OnboardingScreen rejectedName={deviceName} />
  }
  return <DeviceWorkspace key={name} name={name} />
}

function DeviceWorkspace({ name }: { name: string }) {
  const { phase, device, view, wantsOnline, setSyncEnabled, dismissBanner, reportError, openError } =
    useFieldDevice(name)
  const navigate = useNavigate()

  const handleCreate = useCallback(
    (site: string, task: string) => {
      if (device === null) return
      void createWorkOrder(device, site, task).catch(reportError)
    },
    [device, reportError],
  )

  const handleClaim = useCallback(
    (id: string) => {
      if (device === null) return
      void claimWorkOrder(device, id).catch(reportError)
    },
    [device, reportError],
  )

  const handleComplete = useCallback(
    (id: string, note: string) => {
      if (device === null) return
      void completeWorkOrder(device, id, note).catch(reportError)
    },
    [device, reportError],
  )

  const handleReopen = useCallback(
    (id: string) => {
      if (device === null) return
      void reopenWorkOrder(device, id).catch(reportError)
    },
    [device, reportError],
  )

  const handleChooseAnotherDevice = useCallback(() => {
    void navigate({ to: '/', search: {} })
  }, [navigate])

  if (phase === 'locked') {
    return <LockedScreen name={name} />
  }

  if (phase === 'failed') {
    return (
      <main className="flex min-h-dvh items-center justify-center px-4">
        <div className="w-full max-w-md space-y-4">
          <Alert variant="destructive">
            <TriangleAlert aria-hidden="true" />
            <AlertTitle>
              Could not open device <span className="font-mono">{name}</span>
            </AlertTitle>
            <AlertDescription>{openError}</AlertDescription>
          </Alert>
          <Button variant="outline" className="w-full" onClick={handleChooseAnotherDevice}>
            Choose another device
          </Button>
        </div>
      </main>
    )
  }

  if (phase === 'opening' || device === null) {
    return (
      <main className="flex min-h-dvh items-center justify-center px-4">
        <p className="text-muted-foreground flex items-center gap-2 text-sm">
          <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
          Opening the local database for <span className="font-mono">{name}</span>…
        </p>
      </main>
    )
  }

  const bannerError = view.bannerError ?? view.status?.lastError?.message ?? null

  return (
    <div className="min-h-dvh">
      <AppHeader deviceName={name} wantsOnline={wantsOnline} onSyncEnabledChange={setSyncEnabled} />
      <main className="mx-auto w-full max-w-6xl px-4 pb-16 sm:px-6">
        <SyncStatusBar status={view.status} changesReceived={view.changesReceived} />
        {bannerError !== null ? (
          <Alert variant="destructive" className="mt-4">
            <TriangleAlert aria-hidden="true" />
            <AlertTitle>Sync is failing</AlertTitle>
            <AlertDescription>{bannerError}</AlertDescription>
            <AlertAction>
              <Button variant="ghost" size="icon-xs" aria-label="Dismiss" onClick={dismissBanner}>
                <X aria-hidden="true" />
              </Button>
            </AlertAction>
          </Alert>
        ) : null}
        {view.snapshotting ? (
          <SnapshotPanel progress={view.snapshotProgress} />
        ) : (
          <OrderBoard
            device={device}
            onCreate={handleCreate}
            onClaim={handleClaim}
            onComplete={handleComplete}
            onReopen={handleReopen}
          />
        )}
      </main>
    </div>
  )
}
