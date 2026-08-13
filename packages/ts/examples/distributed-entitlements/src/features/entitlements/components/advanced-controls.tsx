import { Card } from '@delali/sirannon-example-shared/ui/card'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@delali/sirannon-example-shared/ui/collapsible'
import { Separator } from '@delali/sirannon-example-shared/ui/separator'
import { ChevronsUpDown } from 'lucide-react'
import { useState } from 'react'
import type { CreateCustomerInput, CustomerEntitlement } from '../../../lib/schemas'
import type { BillingDraft } from '../types'
import { BillingForm } from './billing-form'
import { CreateCustomerForm } from './create-customer-form'

export function AdvancedControls({
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
