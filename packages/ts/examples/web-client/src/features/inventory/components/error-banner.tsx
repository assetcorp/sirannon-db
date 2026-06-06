import { ShieldCheck } from 'lucide-react'

export function ErrorBanner({ message, onDismiss }: { message: string; onDismiss: () => void }) {
  return (
    <div className="error-banner" role="alert" aria-live="assertive" aria-atomic="true">
      <ShieldCheck size={18} />
      <p>{message}</p>
      <button type="button" onClick={onDismiss}>
        Dismiss
      </button>
    </div>
  )
}
