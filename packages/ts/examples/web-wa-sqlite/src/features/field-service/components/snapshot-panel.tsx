import type { SnapshotProgress } from '@delali/sirannon-db/client'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@delali/sirannon-example-shared/ui/card'
import { Progress } from '@delali/sirannon-example-shared/ui/progress'
import { DatabaseZap } from 'lucide-react'

export function SnapshotPanel({ progress }: { progress: SnapshotProgress | null }) {
  const percent =
    progress !== null && progress.totalRows > 0 ? Math.round((progress.loadedRows / progress.totalRows) * 100) : null

  return (
    <div className="mt-10 flex justify-center">
      <Card className="animate-rise w-full max-w-md">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="bg-primary/10 text-primary flex size-9 items-center justify-center rounded-md">
              <DatabaseZap className="size-4.5" aria-hidden="true" />
            </div>
            <div>
              <CardTitle>Downloading snapshot</CardTitle>
              <CardDescription>Replacing this device's local copy with the server's data.</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-2">
          <Progress value={percent} />
          <p className="text-muted-foreground font-mono text-xs tabular-nums">
            {progress === null
              ? 'Waiting for the first page…'
              : `${progress.table}: ${progress.loadedRows} of ${progress.totalRows} rows`}
          </p>
        </CardContent>
      </Card>
    </div>
  )
}
