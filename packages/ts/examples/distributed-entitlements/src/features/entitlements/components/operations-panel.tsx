import { FlameKindling, Link2Off, Plus, Repeat2, Send, Wrench } from 'lucide-react'
import { type ChangeEvent, type FormEvent, useCallback, useEffect, useState } from 'react'
import type { CreateCustomerInput, CustomerEntitlement, Plan, SupportTier } from '../../../lib/schemas'
import { createIdempotencyKey, nextBillingVersion } from '../entitlements-utils'
import type { BillingDraft, UsageDraft } from '../types'
import { PanelHeader } from './panel-header'

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
  return (
    <aside className="ops-panel command-panel">
      <PanelHeader icon={<FlameKindling size={18} />} title="Operator Console" />
      <CreateCustomerForm disabled={pendingAction !== null} onSubmit={onCreateCustomer} />
      <UsageForm
        selectedCustomer={selectedCustomer}
        disabled={pendingAction !== null}
        onSubmit={onRecordUsage}
        onReplayDuplicateUsage={onReplayDuplicateUsage}
      />
      <BillingForm selectedCustomer={selectedCustomer} disabled={pendingAction !== null} onSubmit={onApplyBilling} />
      <FaultControls
        disabled={pendingAction !== null}
        onIsolatePrimary={onIsolatePrimary}
        onHealCluster={onHealCluster}
      />
    </aside>
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
      <button className="primary-button" type="submit" disabled={disabled}>
        <Plus size={15} />
        Create
      </button>
    </form>
  )
}

function UsageForm({
  selectedCustomer,
  disabled,
  onSubmit,
  onReplayDuplicateUsage,
}: {
  selectedCustomer: CustomerEntitlement | null
  disabled: boolean
  onSubmit: (draft: UsageDraft) => Promise<boolean>
  onReplayDuplicateUsage: () => Promise<boolean>
}) {
  const [units, setUnits] = useState(500)
  const [idempotencyKey, setIdempotencyKey] = useState('usage-pulse')

  useEffect(() => {
    if (selectedCustomer) {
      setIdempotencyKey(createIdempotencyKey('usage', selectedCustomer.id))
    }
  }, [selectedCustomer])

  const handleUnitsChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setUnits(Number(event.currentTarget.value))
  }, [])

  const handleIdempotencyKeyChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setIdempotencyKey(event.currentTarget.value)
  }, [])

  const handleSubmit = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      const recorded = await onSubmit({
        units,
        source: 'api_gateway',
        idempotencyKey,
      })
      if (recorded && selectedCustomer) {
        setIdempotencyKey(createIdempotencyKey('usage', selectedCustomer.id))
      }
    },
    [idempotencyKey, onSubmit, selectedCustomer, units],
  )

  const handleReplayClick = useCallback(() => {
    void onReplayDuplicateUsage()
  }, [onReplayDuplicateUsage])

  return (
    <form className="operator-form" onSubmit={handleSubmit}>
      <h3>Usage ingestion</h3>
      <div className="selected-target">{selectedCustomer ? selectedCustomer.name : 'No account selected'}</div>
      <div className="form-grid">
        <label>
          Units
          <input type="number" min={1} max={100000} value={units} disabled={disabled} onChange={handleUnitsChange} />
        </label>
        <label>
          Idempotency key
          <input type="text" value={idempotencyKey} disabled={disabled} onChange={handleIdempotencyKeyChange} />
        </label>
      </div>
      <div className="button-row">
        <button className="primary-button" type="submit" disabled={disabled || !selectedCustomer}>
          <Send size={15} />
          Record
        </button>
        <button
          className="secondary-button"
          type="button"
          disabled={disabled || !selectedCustomer}
          onClick={handleReplayClick}
        >
          <Repeat2 size={15} />
          Replay duplicate
        </button>
      </div>
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
      <h3>Billing webhook</h3>
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
      <button className="primary-button" type="submit" disabled={disabled || !selectedCustomer}>
        <Send size={15} />
        Apply event
      </button>
    </form>
  )
}

function FaultControls({
  disabled,
  onIsolatePrimary,
  onHealCluster,
}: {
  disabled: boolean
  onIsolatePrimary: () => Promise<void>
  onHealCluster: () => Promise<void>
}) {
  const handleIsolateClick = useCallback(() => {
    void onIsolatePrimary()
  }, [onIsolatePrimary])

  const handleHealClick = useCallback(() => {
    void onHealCluster()
  }, [onHealCluster])

  return (
    <div className="operator-form fault-controls">
      <h3>Failure controls</h3>
      <div className="button-row">
        <button className="danger-button" type="button" disabled={disabled} onClick={handleIsolateClick}>
          <Link2Off size={15} />
          Isolate primary
        </button>
        <button className="secondary-button" type="button" disabled={disabled} onClick={handleHealClick}>
          <Wrench size={15} />
          Heal links
        </button>
      </div>
    </div>
  )
}
