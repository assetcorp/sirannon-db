import { X } from 'lucide-react'
import { useCallback } from 'react'

export function ErrorBanner({ message, onDismiss }: { message: string; onDismiss: () => void }) {
  const handleDismiss = useCallback(() => {
    onDismiss()
  }, [onDismiss])

  return (
    <div className="error-banner" role="alert" aria-live="assertive" aria-atomic="true">
      <span>{message}</span>
      <button type="button" onClick={handleDismiss} aria-label="Dismiss error" title="Dismiss error">
        <X size={16} />
      </button>
    </div>
  )
}
