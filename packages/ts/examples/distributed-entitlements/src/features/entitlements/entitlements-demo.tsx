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
    <main className="app-shell">
      <AppHeader
        connectionState={controller.connectionState}
        refreshing={controller.refreshing}
        pendingAction={controller.pendingAction}
        lastEvent={controller.lastEvent}
        onRefresh={controller.handleRefreshClick}
        onReset={controller.handleResetClick}
      />

      <MetricsGrid stats={controller.stats} />
      <ClusterBoard nodes={controller.clusterNodes} />

      {controller.error ? <ErrorBanner message={controller.error} onDismiss={controller.handleDismissError} /> : null}

      <section className="workspace-grid">
        <CustomerTable
          customers={controller.customers}
          selectedCustomer={controller.selectedCustomer}
          pendingAction={controller.pendingAction}
          onSelectCustomer={controller.handleSelectCustomer}
        />
        <div className="record-workspace">
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
          onCreateCustomer={controller.handleCreateCustomer}
          onRecordUsage={controller.handleRecordUsage}
          onReplayDuplicateUsage={controller.handleReplayDuplicateUsage}
          onApplyBilling={controller.handleApplyBilling}
          onIsolatePrimary={controller.handleIsolatePrimary}
          onHealCluster={controller.handleHealCluster}
        />
      </section>
    </main>
  )
}
