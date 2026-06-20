import { AlertCircle, X } from 'lucide-react'
import { useCallback } from 'react'
import { Alert, AlertAction, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'

export function ErrorBanner({ message, onDismiss }: { message: string; onDismiss: () => void }) {
  const handleDismiss = useCallback(() => {
    onDismiss()
  }, [onDismiss])

  return (
    <Alert variant="destructive" className="border-destructive/40 bg-destructive/10">
      <AlertCircle aria-hidden="true" />
      <AlertTitle>Operation failed</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
      <AlertAction>
        <Button
          variant="ghost"
          size="icon-sm"
          type="button"
          onClick={handleDismiss}
          aria-label="Dismiss error"
          className="text-destructive hover:bg-destructive/15 hover:text-destructive"
        >
          <X aria-hidden="true" />
        </Button>
      </AlertAction>
    </Alert>
  )
}
