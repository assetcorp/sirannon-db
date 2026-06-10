import { ArrowUpRight, Gauge, Link2Off, Plus, ReceiptText, Repeat2, Send, Wrench } from 'lucide-react'
import { type ChangeEvent, type FormEvent, useCallback, useEffect, useState } from 'react'
import type { CreateCustomerInput, CustomerEntitlement, Plan, SupportTier } from '../../../lib/schemas'
import { createIdempotencyKey, formatCompactNumber, nextBillingVersion } from '../entitlements-utils'
import type { BillingDraft, UsageDraft } from '../types'
import { Badge } from './ui/badge'
import { Button } from './ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card'

type OperationsMode = 'operate' | 'incident'

export function OperationsPanel({
  selectedCustomer,
  pendingAction,
  onCreateCustomer,
  onRecordUsage,
  onReplayDuplicateUsage,
  onApplyBilling,
  onIsolatePrimary,
  onHealCluster,
}: {
  selectedCustomer: CustomerEntitlement | null
  pendingAction: string | null
  onCreateCustomer: (input: CreateCustomerInput) => Promise<boolean>
  onRecordUsage: (draft: UsageDraft) => Promise<boolean>
  onReplayDuplicateUsage: () => Promise<boolean>
  onApplyBilling: (draft: BillingDraft) => Promise<boolean>
  onIsolatePrimary: () => Promise<void>
  onHealCluster: () => Promise<void>
}) {
  const [mode, setMode] = useState<OperationsMode>('operate')
  const disabled = pendingAction !== null || selectedCustomer === null

  const handleOperateClick = useCallback(() => {
    setMode('operate')
  }, [])

  const handleIncidentClick = useCallback(() => {
    setMode('incident')
  }, [])

  const handleConsumeClick = useCallback(() => {
    if (!selectedCustomer) {
      return
    }
    void onRecordUsage({
      units: 500,
      source: 'api_gateway',
      idempotencyKey: createIdempotencyKey('usage', selectedCustomer.id),
    })
  }, [onRecordUsage, selectedCustomer])

  const handleReplayClick = useCallback(() => {
    void onReplayDuplicateUsage()
  }, [onReplayDuplicateUsage])

  const handleUpgradeClick = useCallback(() => {
    if (!selectedCustomer) {
      return
    }
    const nextPlan = nextUpgradePlan(selectedCustomer.plan)
    void onApplyBilling({
      providerEventId: `evt_upgrade_${Date.now().toString(36)}`,
      eventType: 'subscription.updated',
      plan: nextPlan,
      status: 'active',
      seats: selectedCustomer.seats + 12,
      apiQuota: selectedCustomer.api_quota + quotaStepForPlan(nextPlan),
      supportTier: supportForPlan(nextPlan),
      active: true,
      version: nextBillingVersion(selectedCustomer),
    })
  }, [onApplyBilling, selectedCustomer])

  const handleStaleBillingClick = useCallback(() => {
    if (!selectedCustomer) {
      return
    }
    void onApplyBilling({
      providerEventId: `evt_stale_${Date.now().toString(36)}`,
      eventType: 'subscription.updated',
      plan: selectedCustomer.plan,
      status: selectedCustomer.status,
      seats: selectedCustomer.seats,
      apiQuota: selectedCustomer.api_quota,
      supportTier: selectedCustomer.support_tier,
      active: selectedCustomer.active === 1,
      version: Math.max(1, selectedCustomer.version - 1),
    })
  }, [onApplyBilling, selectedCustomer])

  const handleIsolateClick = useCallback(() => {
    void onIsolatePrimary()
  }, [onIsolatePrimary])

  const handleHealClick = useCallback(() => {
    void onHealCluster()
  }, [onHealCluster])

  const content =
    mode === 'operate' ? (
      <OperationCommands
        disabled={disabled}
        selectedCustomer={selectedCustomer}
        onConsume={handleConsumeClick}
        onReplay={handleReplayClick}
        onUpgrade={handleUpgradeClick}
        onStaleBilling={handleStaleBillingClick}
      />
    ) : (
      <IncidentCommands disabled={pendingAction !== null} onIsolate={handleIsolateClick} onHeal={handleHealClick} />
    )

  return (
    <aside className="command-rail">
      <Card>
        <CardHeader className="command-header">
          <div>
            <CardTitle>Command Center</CardTitle>
            <CardDescription>
              {selectedCustomer ? selectedCustomer.name : 'Select an account to run entitlement operations'}
            </CardDescription>
          </div>
          {pendingAction ? <Badge variant="warning">{pendingAction}</Badge> : <Badge variant="secondary">ready</Badge>}
        </CardHeader>
        <CardContent>
          <div className="segmented-control" role="tablist" aria-label="Command mode">
            <button
              className={mode === 'operate' ? 'segment active' : 'segment'}
              type="button"
              onClick={handleOperateClick}
              aria-selected={mode === 'operate'}
              role="tab"
            >
              Operations
            </button>
            <button
              className={mode === 'incident' ? 'segment active' : 'segment'}
              type="button"
              onClick={handleIncidentClick}
              aria-selected={mode === 'incident'}
              role="tab"
            >
              Incident
            </button>
          </div>
          {content}
        </CardContent>
      </Card>
      <AdvancedControls
        disabled={pendingAction !== null}
        selectedCustomer={selectedCustomer}
        onCreateCustomer={onCreateCustomer}
        onApplyBilling={onApplyBilling}
      />
    </aside>
  )
}

function OperationCommands({
  disabled,
  selectedCustomer,
  onConsume,
  onReplay,
  onUpgrade,
  onStaleBilling,
}: {
  disabled: boolean
  selectedCustomer: CustomerEntitlement | null
  onConsume: () => void
  onReplay: () => void
  onUpgrade: () => void
  onStaleBilling: () => void
}) {
  const quotaLabel = selectedCustomer ? formatCompactNumber(selectedCustomer.api_quota) : 'none'

  return (
    <div className="command-stack">
      <CommandButton
        icon={<Gauge size={16} />}
        title="Consume 500 API units"
        detail={`Current quota ${quotaLabel}`}
        disabled={disabled}
        onClick={onConsume}
      />
      <CommandButton
        icon={<Repeat2 size={16} />}
        title="Replay same usage event"
        detail="Same idempotency key, one quota change"
        disabled={disabled}
        onClick={onReplay}
      />
      <CommandButton
        icon={<ArrowUpRight size={16} />}
        title="Upgrade entitlement"
        detail="New billing version, seats and quota increase"
        disabled={disabled}
        onClick={onUpgrade}
      />
      <CommandButton
        icon={<ReceiptText size={16} />}
        title="Send stale billing event"
        detail="Older version should be recorded as stale"
        disabled={disabled}
        onClick={onStaleBilling}
      />
    </div>
  )
}

function IncidentCommands({
  disabled,
  onIsolate,
  onHeal,
}: {
  disabled: boolean
  onIsolate: () => void
  onHeal: () => void
}) {
  return (
    <div className="command-stack">
      <CommandButton
        icon={<Link2Off size={16} />}
        title="Partition current primary"
        detail="Disable its coordinator proxy"
        disabled={disabled}
        dangerous
        onClick={onIsolate}
      />
      <CommandButton
        icon={<Wrench size={16} />}
        title="Heal cluster links"
        detail="Restore coordinator and replication proxies"
        disabled={disabled}
        onClick={onHeal}
      />
    </div>
  )
}

function CommandButton({
  icon,
  title,
  detail,
  disabled,
  dangerous = false,
  onClick,
}: {
  icon: React.ReactNode
  title: string
  detail: string
  disabled: boolean
  dangerous?: boolean
  onClick: () => void
}) {
  return (
    <button
      className={dangerous ? 'command-button dangerous' : 'command-button'}
      type="button"
      disabled={disabled}
      onClick={onClick}
    >
      <span className="command-icon">{icon}</span>
      <span className="command-copy">
        <strong>{title}</strong>
        <span>{detail}</span>
      </span>
    </button>
  )
}

function AdvancedControls({
  disabled,
  selectedCustomer,
  onCreateCustomer,
  onApplyBilling,
}: {
  disabled: boolean
  selectedCustomer: CustomerEntitlement | null
  onCreateCustomer: (input: CreateCustomerInput) => Promise<boolean>
  onApplyBilling: (draft: BillingDraft) => Promise<boolean>
}) {
  return (
    <Card className="advanced-card">
      <details>
        <summary>
          <span>Advanced inputs</span>
          <Badge variant="outline">manual</Badge>
        </summary>
        <div className="advanced-grid">
          <CreateCustomerForm disabled={disabled} onSubmit={onCreateCustomer} />
          <BillingForm selectedCustomer={selectedCustomer} disabled={disabled} onSubmit={onApplyBilling} />
        </div>
      </details>
    </Card>
  )
}

function CreateCustomerForm({
  disabled,
  onSubmit,
}: {
  disabled: boolean
  onSubmit: (input: CreateCustomerInput) => Promise<boolean>
}) {
  const [name, setName] = useState('Atlas Metrics')
  const [plan, setPlan] = useState<Plan>('growth')
  const [supportTier, setSupportTier] = useState<SupportTier>('standard')
  const [seats, setSeats] = useState(24)
  const [apiQuota, setApiQuota] = useState(125000)

  const handleNameChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setName(event.currentTarget.value)
  }, [])

  const handlePlanChange = useCallback((event: ChangeEvent<HTMLSelectElement>) => {
    setPlan(event.currentTarget.value as Plan)
  }, [])

  const handleSupportTierChange = useCallback((event: ChangeEvent<HTMLSelectElement>) => {
    setSupportTier(event.currentTarget.value as SupportTier)
  }, [])

  const handleSeatsChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setSeats(Number(event.currentTarget.value))
  }, [])

  const handleApiQuotaChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setApiQuota(Number(event.currentTarget.value))
  }, [])

  const handleSubmit = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      const created = await onSubmit({ name, plan, supportTier, seats, apiQuota })
      if (created) {
        setName('Atlas Metrics')
      }
    },
    [apiQuota, name, onSubmit, plan, seats, supportTier],
  )

  return (
    <form className="operator-form" onSubmit={handleSubmit}>
      <h3>Create account</h3>
      <div className="form-grid two">
        <label>
          Name
          <input type="text" value={name} disabled={disabled} onChange={handleNameChange} />
        </label>
        <label>
          Plan
          <select value={plan} disabled={disabled} onChange={handlePlanChange}>
            <option value="free">free</option>
            <option value="growth">growth</option>
            <option value="scale">scale</option>
            <option value="enterprise">enterprise</option>
          </select>
        </label>
        <label>
          Seats
          <input type="number" min={1} max={1000} value={seats} disabled={disabled} onChange={handleSeatsChange} />
        </label>
        <label>
          API quota
          <input
            type="number"
            min={1000}
            max={10000000}
            value={apiQuota}
            disabled={disabled}
            onChange={handleApiQuotaChange}
          />
        </label>
        <label className="span-two">
          Support tier
          <select value={supportTier} disabled={disabled} onChange={handleSupportTierChange}>
            <option value="community">community</option>
            <option value="standard">standard</option>
            <option value="priority">priority</option>
            <option value="named">named</option>
          </select>
        </label>
      </div>
      <Button type="submit" disabled={disabled} size="sm">
        <Plus size={15} />
        Create
      </Button>
    </form>
  )
}

function BillingForm({
  selectedCustomer,
  disabled,
  onSubmit,
}: {
  selectedCustomer: CustomerEntitlement | null
  disabled: boolean
  onSubmit: (draft: BillingDraft) => Promise<boolean>
}) {
  const [eventType, setEventType] = useState<BillingDraft['eventType']>('subscription.updated')
  const [plan, setPlan] = useState<Plan>('scale')
  const [supportTier, setSupportTier] = useState<SupportTier>('priority')
  const [status, setStatus] = useState<BillingDraft['status']>('active')
  const [seats, setSeats] = useState(64)
  const [apiQuota, setApiQuota] = useState(300000)
  const [active, setActive] = useState(true)
  const [version, setVersion] = useState(1)

  useEffect(() => {
    if (!selectedCustomer) {
      return
    }
    setPlan(selectedCustomer.plan)
    setSupportTier(selectedCustomer.support_tier)
    setStatus(selectedCustomer.status)
    setSeats(selectedCustomer.seats)
    setApiQuota(selectedCustomer.api_quota)
    setActive(selectedCustomer.active === 1)
    setVersion(nextBillingVersion(selectedCustomer))
  }, [selectedCustomer])

  const handleEventTypeChange = useCallback((event: ChangeEvent<HTMLSelectElement>) => {
    setEventType(event.currentTarget.value as BillingDraft['eventType'])
  }, [])

  const handlePlanChange = useCallback((event: ChangeEvent<HTMLSelectElement>) => {
    setPlan(event.currentTarget.value as Plan)
  }, [])

  const handleSupportTierChange = useCallback((event: ChangeEvent<HTMLSelectElement>) => {
    setSupportTier(event.currentTarget.value as SupportTier)
  }, [])

  const handleStatusChange = useCallback((event: ChangeEvent<HTMLSelectElement>) => {
    setStatus(event.currentTarget.value as BillingDraft['status'])
  }, [])

  const handleSeatsChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setSeats(Number(event.currentTarget.value))
  }, [])

  const handleApiQuotaChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setApiQuota(Number(event.currentTarget.value))
  }, [])

  const handleActiveChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setActive(event.currentTarget.checked)
  }, [])

  const handleVersionChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setVersion(Number(event.currentTarget.value))
  }, [])

  const handleSubmit = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      const applied = await onSubmit({
        providerEventId: `evt_${Date.now().toString(36)}_${version}`,
        eventType,
        plan,
        status,
        seats,
        apiQuota,
        supportTier,
        active,
        version,
      })
      if (applied && selectedCustomer) {
        setVersion(nextBillingVersion({ ...selectedCustomer, version }))
      }
    },
    [active, apiQuota, eventType, onSubmit, plan, seats, selectedCustomer, status, supportTier, version],
  )

  return (
    <form className="operator-form" onSubmit={handleSubmit}>
      <h3>Manual billing event</h3>
      <div className="form-grid two">
        <label>
          Event
          <select value={eventType} disabled={disabled} onChange={handleEventTypeChange}>
            <option value="subscription.created">subscription.created</option>
            <option value="subscription.updated">subscription.updated</option>
            <option value="invoice.payment_failed">invoice.payment_failed</option>
          </select>
        </label>
        <label>
          Version
          <input
            type="number"
            min={1}
            max={1000000}
            value={version}
            disabled={disabled}
            onChange={handleVersionChange}
          />
        </label>
        <label>
          Plan
          <select value={plan} disabled={disabled} onChange={handlePlanChange}>
            <option value="free">free</option>
            <option value="growth">growth</option>
            <option value="scale">scale</option>
            <option value="enterprise">enterprise</option>
          </select>
        </label>
        <label>
          Status
          <select value={status} disabled={disabled} onChange={handleStatusChange}>
            <option value="active">active</option>
            <option value="past_due">past_due</option>
            <option value="suspended">suspended</option>
          </select>
        </label>
        <label>
          Seats
          <input type="number" min={0} max={1000} value={seats} disabled={disabled} onChange={handleSeatsChange} />
        </label>
        <label>
          API quota
          <input
            type="number"
            min={0}
            max={10000000}
            value={apiQuota}
            disabled={disabled}
            onChange={handleApiQuotaChange}
          />
        </label>
        <label className="span-two">
          Support tier
          <select value={supportTier} disabled={disabled} onChange={handleSupportTierChange}>
            <option value="community">community</option>
            <option value="standard">standard</option>
            <option value="priority">priority</option>
            <option value="named">named</option>
          </select>
        </label>
        <label className="checkbox-line span-two">
          <input type="checkbox" checked={active} disabled={disabled} onChange={handleActiveChange} />
          Active entitlements
        </label>
      </div>
      <Button type="submit" disabled={disabled || !selectedCustomer} size="sm">
        <Send size={15} />
        Apply
      </Button>
    </form>
  )
}

function nextUpgradePlan(plan: Plan): Plan {
  if (plan === 'free') {
    return 'growth'
  }
  if (plan === 'growth') {
    return 'scale'
  }
  return 'enterprise'
}

function supportForPlan(plan: Plan): SupportTier {
  if (plan === 'enterprise') {
    return 'named'
  }
  if (plan === 'scale') {
    return 'priority'
  }
  if (plan === 'growth') {
    return 'standard'
  }
  return 'community'
}

function quotaStepForPlan(plan: Plan): number {
  if (plan === 'enterprise') {
    return 250000
  }
  if (plan === 'scale') {
    return 100000
  }
  return 25000
}
