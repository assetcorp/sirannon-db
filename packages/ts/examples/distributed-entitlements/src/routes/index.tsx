import { createFileRoute } from '@tanstack/react-router'
import { EntitlementsDemo } from '../features/entitlements/entitlements-demo'
import { toErrorMessage } from '../features/entitlements/entitlements-utils'
import { EMPTY_CONTROL_PLANE, type LoaderData } from '../features/entitlements/types'
import { getControlPlaneSnapshot } from '../lib/app-actions.functions'

async function loadControlPlane(): Promise<LoaderData> {
  try {
    const snapshot = await getControlPlaneSnapshot()
    return { ...snapshot, initialError: null }
  } catch (error) {
    return {
      ...EMPTY_CONTROL_PLANE,
      initialError: toErrorMessage(error),
    }
  }
}

export const Route = createFileRoute('/')({
  component: EntitlementsRoute,
  loader: loadControlPlane,
})

function EntitlementsRoute() {
  const initialData = Route.useLoaderData()
  return <EntitlementsDemo initialData={initialData} />
}
