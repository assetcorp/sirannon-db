import { AppHeader } from './components/app-header'
import { ClusterBoard } from './components/cluster-board'
import { CustomerTable } from './components/customer-table'
import { EntitlementDetail } from './components/entitlement-detail'
import { ErrorBanner } from './components/error-banner'
import { ActivityTimeline } from './components/event-stream'
import { MetricsGrid } from './components/metrics-grid'
import { OperationsPanel } from './components/operations-panel'
import type { LoaderData } from './types'
import { useEntitlementsController } from './use-entitlements-controller'

export function EntitlementsDemo({ initialData }: { initialData: LoaderData }) {
  const controller = useEntitlementsController(initialData)

  return (
    <div className="min-h-dvh">
      <AppHeader
        connectionState={controller.connectionState}
        refreshing={controller.refreshing}
        pendingAction={controller.pendingAction}
        writeAvailable={controller.writeAvailability.available}
        writeUnavailableReason={controller.writeAvailability.reason}
        lastEvent={controller.lastEvent}
        onRefresh={controller.handleRefreshClick}
        onReset={controller.handleResetClick}
      />

      <main className="mx-auto flex w-full max-w-[1440px] flex-col gap-4 px-4 py-5 lg:px-6">
        <div className="animate-rise motion-reduce:animate-none">
          <MetricsGrid stats={controller.stats} />
        </div>
        <div className="animate-rise motion-reduce:animate-none [animation-delay:60ms]">
          <ClusterBoard nodes={controller.clusterNodes} />
        </div>

        {controller.error ? <ErrorBanner message={controller.error} onDismiss={controller.handleDismissError} /> : null}

        <section
          aria-label="Entitlement workspace"
          className="animate-rise grid items-start gap-4 motion-reduce:animate-none [animation-delay:120ms] xl:grid-cols-[300px_minmax(0,1fr)_360px]"
        >
          <CustomerTable
            customers={controller.customers}
            selectedCustomer={controller.selectedCustomer}
            pendingAction={controller.pendingAction}
            onSelectCustomer={controller.handleSelectCustomer}
          />
          <div className="flex min-w-0 flex-col gap-4">
            <EntitlementDetail customer={controller.selectedCustomer} />
            <ActivityTimeline
              selectedCustomer={controller.selectedCustomer}
              usage={controller.usage}
              billingEvents={controller.billingEvents}
              auditLog={controller.auditLog}
            />
          </div>
          <OperationsPanel
            selectedCustomer={controller.selectedCustomer}
            pendingAction={controller.pendingAction}
            writeAvailable={controller.writeAvailability.available}
            writeUnavailableReason={controller.writeAvailability.reason}
            onCreateCustomer={controller.handleCreateCustomer}
            onRecordUsage={controller.handleRecordUsage}
            onReplayDuplicateUsage={controller.handleReplayDuplicateUsage}
            onApplyBilling={controller.handleApplyBilling}
            onIsolatePrimary={controller.handleIsolatePrimary}
            onHealCluster={controller.handleHealCluster}
          />
        </section>
      </main>
    </div>
  )
}
