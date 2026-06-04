import { Activity, Database, Gauge, Network, ShieldCheck } from 'lucide-react'
import { AppHeader } from './components/app-header'
import { ClusterBoard } from './components/cluster-board'
import { CustomerTable } from './components/customer-table'
import { ErrorBanner } from './components/error-banner'
import { EventStream } from './components/event-stream'
import { MetricsGrid } from './components/metrics-grid'
import { OperationsPanel } from './components/operations-panel'
import { PanelHeader } from './components/panel-header'
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

      {controller.error ? <ErrorBanner message={controller.error} onDismiss={controller.handleDismissError} /> : null}

      <section className="command-grid">
        <div className="primary-workspace">
          <PanelHeader icon={<ShieldCheck size={18} />} title="Entitlements Ledger" />
          <CustomerTable
            customers={controller.customers}
            selectedCustomer={controller.selectedCustomer}
            pendingAction={controller.pendingAction}
            onSelectCustomer={controller.handleSelectCustomer}
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

      <section className="observability-grid">
        <div className="ops-panel">
          <PanelHeader icon={<Network size={18} />} title="Cluster Authority" />
          <ClusterBoard nodes={controller.clusterNodes} />
        </div>
        <EventStream icon={<Gauge size={18} />} title="Usage Ingestion" kind="usage" records={controller.usage} />
        <EventStream
          icon={<Activity size={18} />}
          title="Billing Events"
          kind="billing"
          records={controller.billingEvents}
        />
        <EventStream icon={<Database size={18} />} title="Audit Log" kind="audit" records={controller.auditLog} />
      </section>
    </main>
  )
}
