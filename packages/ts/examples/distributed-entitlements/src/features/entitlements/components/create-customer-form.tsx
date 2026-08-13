import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Input } from '@delali/sirannon-example-shared/ui/input'
import { Select, SelectContent, SelectTrigger, SelectValue } from '@delali/sirannon-example-shared/ui/select'
import { Plus } from 'lucide-react'
import { type ChangeEvent, type FormEvent, useCallback, useId, useState } from 'react'
import type { CreateCustomerInput, Plan, SupportTier } from '../../../lib/schemas'
import { FormField, PLAN_OPTIONS, renderOption, SUPPORT_OPTIONS } from './form-field'

export function CreateCustomerForm({
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
