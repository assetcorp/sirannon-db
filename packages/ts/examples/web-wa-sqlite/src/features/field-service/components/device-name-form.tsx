import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Input } from '@delali/sirannon-example-shared/ui/input'
import { Label } from '@delali/sirannon-example-shared/ui/label'
import { Plus } from 'lucide-react'
import { type ChangeEvent, type FormEvent, useCallback, useId, useState } from 'react'
import { DEVICE_NAME_RULE, isValidDeviceName, normaliseDeviceName } from '../../../lib/device-registry'

export function DeviceNameForm({ label, onPick }: { label: string; onPick: (name: string) => void }) {
  const inputId = useId()
  const [draft, setDraft] = useState('')
  const [invalid, setInvalid] = useState(false)

  const handleDraftChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setDraft(event.target.value)
    setInvalid(false)
  }, [])

  const handleSubmit = useCallback(
    (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      const name = normaliseDeviceName(draft)
      if (!isValidDeviceName(name)) {
        setInvalid(true)
        return
      }
      onPick(name)
    },
    [draft, onPick],
  )

  return (
    <form className="space-y-2" onSubmit={handleSubmit}>
      <Label htmlFor={inputId}>{label}</Label>
      <div className="flex gap-2">
        <Input
          id={inputId}
          value={draft}
          onChange={handleDraftChange}
          placeholder="van-1"
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          aria-invalid={invalid}
        />
        <Button type="submit">
          <Plus data-icon="inline-start" aria-hidden="true" />
          Open
        </Button>
      </div>
      <p className={invalid ? 'text-destructive text-xs' : 'text-muted-foreground text-xs'}>{DEVICE_NAME_RULE}</p>
    </form>
  )
}
