import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Input } from '@delali/sirannon-example-shared/ui/input'
import { Label } from '@delali/sirannon-example-shared/ui/label'
import { Select, SelectContent, SelectTrigger, SelectValue } from '@delali/sirannon-example-shared/ui/select'
import { Switch } from '@delali/sirannon-example-shared/ui/switch'
import { Send } from 'lucide-react'
import { type ChangeEvent, type FormEvent, useCallback, useEffect, useId, useState } from 'react'
import type { CustomerEntitlement, Plan, SupportTier } from '../../../lib/schemas'
import { nextBillingVersion } from '../entitlements-utils'
import type { BillingDraft } from '../types'
import { FormField, PLAN_OPTIONS, renderOption, SUPPORT_OPTIONS } from './form-field'

const STATUS_OPTIONS: BillingDraft['status'][] = ['active', 'past_due', 'suspended']
const EVENT_TYPE_OPTIONS: BillingDraft['eventType'][] = [
  'subscription.created',
  'subscription.updated',
  'invoice.payment_failed',
]

export function BillingForm({
  selectedCustomer,
  disabled,
  onSubmit,
}: {
  selectedCustomer: CustomerEntitlement | null
  disabled: boolean
  onSubmit: (draft: BillingDraft) => Promise<boolean>
}) {
  const formId = useId()
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

  const handleEventTypeChange = useCallback((value: string) => {
    setEventType(value as BillingDraft['eventType'])
  }, [])

  const handlePlanChange = useCallback((value: string) => {
    setPlan(value as Plan)
  }, [])

  const handleSupportTierChange = useCallback((value: string) => {
    setSupportTier(value as SupportTier)
  }, [])

  const handleStatusChange = useCallback((value: string) => {
    setStatus(value as BillingDraft['status'])
  }, [])

  const handleSeatsChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setSeats(Number(event.currentTarget.value))
  }, [])

  const handleApiQuotaChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setApiQuota(Number(event.currentTarget.value))
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
    <form className="flex flex-col gap-3" onSubmit={handleSubmit}>
      <h3 className="text-sm font-medium">Manual billing event</h3>
      <div className="grid grid-cols-2 gap-3">
        <FormField id={`${formId}-event`} label="Event" className="col-span-2">
          <Select value={eventType} disabled={disabled} onValueChange={handleEventTypeChange}>
            <SelectTrigger id={`${formId}-event`} className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>{EVENT_TYPE_OPTIONS.map(renderOption)}</SelectContent>
          </Select>
        </FormField>
        <FormField id={`${formId}-version`} label="Version">
          <Input
            id={`${formId}-version`}
            type="number"
            min={1}
            max={1000000}
            value={version}
            disabled={disabled}
            required
            onChange={handleVersionChange}
          />
        </FormField>
        <FormField id={`${formId}-status`} label="Status">
          <Select value={status} disabled={disabled} onValueChange={handleStatusChange}>
            <SelectTrigger id={`${formId}-status`} className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>{STATUS_OPTIONS.map(renderOption)}</SelectContent>
          </Select>
        </FormField>
        <FormField id={`${formId}-plan`} label="Plan">
          <Select value={plan} disabled={disabled} onValueChange={handlePlanChange}>
            <SelectTrigger id={`${formId}-plan`} className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>{PLAN_OPTIONS.map(renderOption)}</SelectContent>
          </Select>
        </FormField>
        <FormField id={`${formId}-support`} label="Support tier">
          <Select value={supportTier} disabled={disabled} onValueChange={handleSupportTierChange}>
            <SelectTrigger id={`${formId}-support`} className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>{SUPPORT_OPTIONS.map(renderOption)}</SelectContent>
          </Select>
        </FormField>
        <FormField id={`${formId}-seats`} label="Seats">
          <Input
            id={`${formId}-seats`}
            type="number"
            min={0}
            max={1000}
            value={seats}
            disabled={disabled}
            required
            onChange={handleSeatsChange}
          />
        </FormField>
        <FormField id={`${formId}-quota`} label="API quota">
          <Input
            id={`${formId}-quota`}
            type="number"
            min={0}
            max={10000000}
            value={apiQuota}
            disabled={disabled}
            required
            onChange={handleApiQuotaChange}
          />
        </FormField>
        <div className="border-border/70 col-span-2 flex items-center justify-between gap-2 rounded-lg border px-3 py-2.5">
          <Label htmlFor={`${formId}-active`} className="text-sm font-normal">
            Active entitlements
          </Label>
          <Switch id={`${formId}-active`} checked={active} disabled={disabled} onCheckedChange={setActive} />
        </div>
      </div>
      <Button type="submit" size="sm" disabled={disabled || !selectedCustomer}>
        <Send aria-hidden="true" />
        Apply billing event
      </Button>
    </form>
  )
}
