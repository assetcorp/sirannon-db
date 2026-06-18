import {
  ArrowUpRight,
  ChevronsUpDown,
  Gauge,
  Link2Off,
  Loader2,
  type LucideIcon,
  Plus,
  ReceiptText,
  Repeat2,
  Send,
  Wrench,
} from 'lucide-react'
import { type ChangeEvent, type FormEvent, type ReactNode, useCallback, useEffect, useId, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Separator } from '@/components/ui/separator'
import { Switch } from '@/components/ui/switch'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { cn } from '@/lib/utils'
import type { CreateCustomerInput, CustomerEntitlement, Plan, SupportTier } from '../../../lib/schemas'
import { createIdempotencyKey, formatCompactNumber, nextBillingVersion } from '../entitlements-utils'
import type { BillingDraft, UsageDraft } from '../types'
import { TONE_BADGE } from './status'

type OperationsMode = 'operate' | 'incident'

const PLAN_OPTIONS: Plan[] = ['free', 'growth', 'scale', 'enterprise']
const SUPPORT_OPTIONS: SupportTier[] = ['community', 'standard', 'priority', 'named']
const STATUS_OPTIONS: BillingDraft['status'][] = ['active', 'past_due', 'suspended']
const EVENT_TYPE_OPTIONS: BillingDraft['eventType'][] = [
  'subscription.created',
  'subscription.updated',
  'invoice.payment_failed',
]

function renderOption(value: string) {
  return (
    <SelectItem key={value} value={value}>
      {value}
    </SelectItem>
  )
}

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

  const handleModeChange = useCallback((value: string) => {
    setMode(value as OperationsMode)
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

  const quotaLabel = selectedCustomer ? formatCompactNumber(selectedCustomer.api_quota) : 'none'

  return (
    <aside className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>Operations</CardTitle>
          <CardDescription>
            {selectedCustomer ? selectedCustomer.name : 'Select an account to run entitlement operations'}
          </CardDescription>
          <CardAction>
            {pendingAction ? (
              <Badge variant="outline" className={cn('max-w-44 gap-1.5 font-mono text-[10px]', TONE_BADGE.warning)}>
                <Loader2 className="animate-spin motion-reduce:animate-none" aria-hidden="true" />
                <span className="truncate">{pendingAction}</span>
              </Badge>
            ) : (
              <Badge variant="outline" className="text-muted-foreground font-mono text-[10px] uppercase">
                ready
              </Badge>
            )}
          </CardAction>
        </CardHeader>
        <CardContent>
          <Tabs value={mode} onValueChange={handleModeChange}>
            <TabsList className="w-full">
              <TabsTrigger value="operate">Operations</TabsTrigger>
              <TabsTrigger value="incident">Incident</TabsTrigger>
            </TabsList>
            <TabsContent value="operate" className="mt-1 flex flex-col gap-2">
              <CommandButton
                icon={Gauge}
                title="Consume 500 API units"
                detail={`Current quota ${quotaLabel}`}
                disabled={disabled}
                onClick={handleConsumeClick}
              />
              <CommandButton
                icon={Repeat2}
                title="Replay same usage event"
                detail="Same idempotency key, one quota change"
                disabled={disabled}
                onClick={handleReplayClick}
              />
              <CommandButton
                icon={ArrowUpRight}
                title="Upgrade entitlement"
                detail="New billing version, seats and quota increase"
                disabled={disabled}
                onClick={handleUpgradeClick}
              />
              <CommandButton
                icon={ReceiptText}
                title="Send stale billing event"
                detail="Older version should be recorded as stale"
                disabled={disabled}
                onClick={handleStaleBillingClick}
              />
            </TabsContent>
            <TabsContent value="incident" className="mt-1 flex flex-col gap-2">
              <CommandButton
                icon={Link2Off}
                title="Partition current primary"
                detail="Disable its coordinator proxy"
                disabled={pendingAction !== null}
                dangerous
                onClick={handleIsolateClick}
              />
              <CommandButton
                icon={Wrench}
                title="Heal cluster links"
                detail="Restore coordinator and replication proxies"
                disabled={pendingAction !== null}
                onClick={handleHealClick}
              />
            </TabsContent>
          </Tabs>
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

function CommandButton({
  icon: Icon,
  title,
  detail,
  disabled,
  dangerous = false,
  onClick,
}: {
  icon: LucideIcon
  title: string
  detail: string
  disabled: boolean
  dangerous?: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={cn(
        'group bg-background/40 hover:border-ring/40 hover:bg-muted/50 focus-visible:ring-ring/50 flex w-full items-start gap-3 rounded-lg border p-3 text-left transition-colors focus-visible:ring-[3px] focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50',
        dangerous && 'border-destructive/30 hover:border-destructive/50 hover:bg-destructive/10',
      )}
    >
      <span
        className={cn(
          'border-border/80 bg-muted/40 text-muted-foreground group-hover:text-foreground mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-md border transition-colors',
          dangerous && 'border-destructive/30 bg-destructive/10 text-destructive group-hover:text-destructive',
        )}
      >
        <Icon className="size-3.5" aria-hidden="true" />
      </span>
      <span className="flex min-w-0 flex-col gap-0.5">
        <span className={cn('text-sm font-medium', dangerous && 'text-destructive')}>{title}</span>
        <span className="text-muted-foreground text-xs">{detail}</span>
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
  const [open, setOpen] = useState(false)

  return (
    <Card className="gap-0 overflow-hidden py-0">
      <Collapsible open={open} onOpenChange={setOpen}>
        <CollapsibleTrigger asChild>
          <button
            type="button"
            className="hover:bg-muted/40 focus-visible:ring-ring/50 flex w-full items-center justify-between gap-2 px-4 py-3.5 text-left transition-colors focus-visible:ring-2 focus-visible:outline-none focus-visible:ring-inset"
          >
            <span className="flex flex-col gap-0.5">
              <span className="text-sm font-medium">Advanced inputs</span>
              <span className="text-muted-foreground text-xs">Create accounts and craft billing events by hand</span>
            </span>
            <ChevronsUpDown className="text-muted-foreground size-4 shrink-0" aria-hidden="true" />
          </button>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <div className="border-border/70 flex flex-col gap-5 border-t px-4 py-4">
            <CreateCustomerForm disabled={disabled} onSubmit={onCreateCustomer} />
            <Separator />
            <BillingForm selectedCustomer={selectedCustomer} disabled={disabled} onSubmit={onApplyBilling} />
          </div>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  )
}

function FormField({
  id,
  label,
  children,
  className,
}: {
  id: string
  label: string
  children: ReactNode
  className?: string
}) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-1.5', className)}>
      <Label htmlFor={id} className="text-muted-foreground text-xs">
        {label}
      </Label>
      {children}
    </div>
  )
}

function CreateCustomerForm({
  disabled,
  onSubmit,
}: {
  disabled: boolean
  onSubmit: (input: CreateCustomerInput) => Promise<boolean>
}) {
  const formId = useId()
  const [name, setName] = useState('Atlas Metrics')
  const [plan, setPlan] = useState<Plan>('growth')
  const [supportTier, setSupportTier] = useState<SupportTier>('standard')
  const [seats, setSeats] = useState(24)
  const [apiQuota, setApiQuota] = useState(125000)

  const handleNameChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setName(event.currentTarget.value)
  }, [])

  const handlePlanChange = useCallback((value: string) => {
    setPlan(value as Plan)
  }, [])

  const handleSupportTierChange = useCallback((value: string) => {
    setSupportTier(value as SupportTier)
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
    <form className="flex flex-col gap-3" onSubmit={handleSubmit}>
      <h3 className="text-sm font-medium">Create account</h3>
      <div className="grid grid-cols-2 gap-3">
        <FormField id={`${formId}-name`} label="Name" className="col-span-2">
          <Input
            id={`${formId}-name`}
            type="text"
            value={name}
            disabled={disabled}
            required
            minLength={2}
            maxLength={80}
            onChange={handleNameChange}
          />
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
            min={1}
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
            min={1000}
            max={10000000}
            value={apiQuota}
            disabled={disabled}
            required
            onChange={handleApiQuotaChange}
          />
        </FormField>
      </div>
      <Button type="submit" size="sm" disabled={disabled}>
        <Plus aria-hidden="true" />
        Create account
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
