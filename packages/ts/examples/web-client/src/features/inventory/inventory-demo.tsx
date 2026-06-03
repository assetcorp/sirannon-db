import { PackagePlus } from 'lucide-react'
import { ActivityPanel } from './components/activity-panel'
import { AddProductForm } from './components/add-product-form'
import { AppHeader } from './components/app-header'
import { ErrorBanner } from './components/error-banner'
import { LiveSignal } from './components/live-signal'
import { MetricsGrid } from './components/metrics-grid'
import { ModeSwitcher } from './components/mode-switcher'
import { PanelHeader } from './components/panel-header'
import { ProductTable } from './components/product-table'
import type { LoaderData } from './types'
import { useInventoryController } from './use-inventory-controller'

export function InventoryDemo({ initialData }: { initialData: LoaderData }) {
  const controller = useInventoryController(initialData)

  return (
    <main className="app-shell">
      <AppHeader
        connectionState={controller.connectionState}
        refreshing={controller.refreshing}
        pendingAction={controller.pendingAction}
        onRefresh={controller.handleRefreshClick}
        onReset={controller.handleResetClick}
      />

      <section className="control-band">
        <ModeSwitcher activeMode={controller.mode} onChange={controller.handleModeChange} />
        <LiveSignal lastEvent={controller.lastEvent} pendingAction={controller.pendingAction} />
      </section>

      <MetricsGrid stats={controller.stats} />

      {controller.error ? <ErrorBanner message={controller.error} onDismiss={controller.handleDismissError} /> : null}

      <section className="workspace-grid">
        <div className="inventory-panel">
          <PanelHeader icon={<PackagePlus size={18} />} title="Inventory Ledger" />
          <AddProductForm disabled={controller.pendingAction !== null} onSubmit={controller.handleAddProduct} />
          <ProductTable
            products={controller.products}
            pendingAction={controller.pendingAction}
            onAllocate={controller.handleAllocateProduct}
            onReceive={controller.handleReceiveInventory}
          />
        </div>
        <ActivityPanel records={controller.activity} />
      </section>
    </main>
  )
}
