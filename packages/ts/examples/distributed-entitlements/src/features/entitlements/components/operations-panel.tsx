import { ArrowUpRight, Gauge, Link2Off, Loader2, ReceiptText, Repeat2, TriangleAlert, Wrench } from 'lucide-react'
import { useCallback, useRef, useState } from 'react'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { cn } from '@/lib/utils'
import type { CreateCustomerInput, CustomerEntitlement, Plan, SupportTier } from '../../../lib/schemas'
import { createIdempotencyKey, formatCompactNumber, nextBillingVersion } from '../entitlements-utils'
import type { BillingDraft, UsageDraft } from '../types'
import { AdvancedControls } from './advanced-controls'
import { CommandButton } from './command-button'
import { TONE_BADGE } from './status'

type OperationsMode = 'operate' | 'incident'

export function OperationsPanel({
  selectedCustomer,
  pendingAction,
  writeAvailable,
  writeUnavailableReason,
  onCreateCustomer,
  onRecordUsage,
  onReplayDuplicateUsage,
  onApplyBilling,
  onIsolatePrimary,
  onHealCluster,
}: {
  selectedCustomer: CustomerEntitlement | null
  pendingAction: string | null
  writeAvailable: boolean
  writeUnavailableReason: string
  onCreateCustomer: (input: CreateCustomerInput) => Promise<boolean>
  onRecordUsage: (draft: UsageDraft) => Promise<boolean>
  onReplayDuplicateUsage: () => Promise<boolean>
  onApplyBilling: (draft: BillingDraft) => Promise<boolean>
  onIsolatePrimary: () => Promise<void>
  onHealCluster: () => Promise<void>
}) {
  const [mode, setMode] = useState<OperationsMode>('operate')
  const usageAttemptKeysRef = useRef(new Map<number, string>())
  const billingAttemptIdsRef = useRef(new Map<string, string>())
  const disabled = pendingAction !== null || selectedCustomer === null || !writeAvailable

  const handleModeChange = useCallback((value: string) => {
    setMode(value as OperationsMode)
  }, [])

  const handleConsumeClick = useCallback(async () => {
    if (!selectedCustomer) {
      return
    }
    const existingKey = usageAttemptKeysRef.current.get(selectedCustomer.id)
    const idempotencyKey = existingKey ?? createIdempotencyKey('usage', selectedCustomer.id)
    usageAttemptKeysRef.current.set(selectedCustomer.id, idempotencyKey)
    const completed = await onRecordUsage({
      units: 500,
      source: 'api_gateway',
      idempotencyKey,
    })
    if (completed) {
      usageAttemptKeysRef.current.delete(selectedCustomer.id)
    }
  }, [onRecordUsage, selectedCustomer])

  const handleReplayClick = useCallback(() => {
    void onReplayDuplicateUsage()
  }, [onReplayDuplicateUsage])

  const handleUpgradeClick = useCallback(async () => {
    if (!selectedCustomer) {
      return
    }
    const attemptKey = `upgrade:${selectedCustomer.id}`
    const existingEventId = billingAttemptIdsRef.current.get(attemptKey)
    const providerEventId = existingEventId ?? `evt_upgrade_${selectedCustomer.id}_${Date.now().toString(36)}`
    billingAttemptIdsRef.current.set(attemptKey, providerEventId)
    const nextPlan = nextUpgradePlan(selectedCustomer.plan)
    const completed = await onApplyBilling({
      providerEventId,
      eventType: 'subscription.updated',
      plan: nextPlan,
      status: 'active',
      seats: selectedCustomer.seats + 12,
      apiQuota: selectedCustomer.api_quota + quotaStepForPlan(nextPlan),
      supportTier: supportForPlan(nextPlan),
      active: true,
      version: nextBillingVersion(selectedCustomer),
    })
    if (completed) {
      billingAttemptIdsRef.current.delete(attemptKey)
    }
  }, [onApplyBilling, selectedCustomer])

  const handleStaleBillingClick = useCallback(async () => {
    if (!selectedCustomer) {
      return
    }
    const attemptKey = `stale:${selectedCustomer.id}`
    const existingEventId = billingAttemptIdsRef.current.get(attemptKey)
    const providerEventId = existingEventId ?? `evt_stale_${selectedCustomer.id}_${Date.now().toString(36)}`
    billingAttemptIdsRef.current.set(attemptKey, providerEventId)
    const completed = await onApplyBilling({
      providerEventId,
      eventType: 'subscription.updated',
      plan: selectedCustomer.plan,
      status: selectedCustomer.status,
      seats: selectedCustomer.seats,
      apiQuota: selectedCustomer.api_quota,
      supportTier: selectedCustomer.support_tier,
      active: selectedCustomer.active === 1,
      version: Math.max(1, selectedCustomer.version - 1),
    })
    if (completed) {
      billingAttemptIdsRef.current.delete(attemptKey)
    }
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
            ) : writeAvailable ? (
              <Badge variant="outline" className="text-muted-foreground font-mono text-[10px] uppercase">
                ready
              </Badge>
            ) : (
              <Badge variant="outline" className={cn('font-mono text-[10px] uppercase', TONE_BADGE.warning)}>
                blocked
              </Badge>
            )}
          </CardAction>
        </CardHeader>
        <CardContent>
          {!writeAvailable ? (
            <Alert className="border-warning/40 bg-warning/5 mb-3">
              <TriangleAlert aria-hidden="true" />
              <AlertTitle>Writes unavailable</AlertTitle>
              <AlertDescription className="text-xs">{writeUnavailableReason}</AlertDescription>
            </Alert>
          ) : null}
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
        disabled={pendingAction !== null || !writeAvailable}
        selectedCustomer={selectedCustomer}
        onCreateCustomer={onCreateCustomer}
        onApplyBilling={onApplyBilling}
      />
    </aside>
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
