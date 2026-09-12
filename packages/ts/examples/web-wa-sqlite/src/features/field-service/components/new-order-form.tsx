import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Card, CardContent } from '@delali/sirannon-example-shared/ui/card'
import { Input } from '@delali/sirannon-example-shared/ui/input'
import { Plus } from 'lucide-react'
import { type ChangeEvent, type FormEvent, useCallback, useState } from 'react'
import { MAX_SITE_LENGTH, MAX_TASK_LENGTH } from '../../../schema'

export function NewOrderForm({ onCreate }: { onCreate: (site: string, task: string) => void }) {
  const [site, setSite] = useState('')
  const [task, setTask] = useState('')

  const handleSiteChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setSite(event.target.value)
  }, [])

  const handleTaskChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    setTask(event.target.value)
  }, [])

  const handleSubmit = useCallback(
    (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      const trimmedSite = site.trim()
      const trimmedTask = task.trim()
      if (trimmedSite.length === 0 || trimmedTask.length === 0) {
        return
      }
      onCreate(trimmedSite, trimmedTask)
      setSite('')
      setTask('')
    },
    [onCreate, site, task],
  )

  return (
    <Card className="border-primary/25 border-dashed py-4 shadow-none">
      <CardContent className="px-4">
        <form className="space-y-2" onSubmit={handleSubmit}>
          <Input
            value={site}
            onChange={handleSiteChange}
            placeholder="Site"
            autoComplete="off"
            maxLength={MAX_SITE_LENGTH}
            className="h-8"
          />
          <Input
            value={task}
            onChange={handleTaskChange}
            placeholder="Task"
            autoComplete="off"
            maxLength={MAX_TASK_LENGTH}
            className="h-8"
          />
          <Button type="submit" size="sm" variant="outline" className="w-full">
            <Plus data-icon="inline-start" aria-hidden="true" />
            Add work order
          </Button>
        </form>
      </CardContent>
    </Card>
  )
}
