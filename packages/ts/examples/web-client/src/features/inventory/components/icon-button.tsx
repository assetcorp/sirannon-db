import type { ReactNode } from 'react'

export function IconButton({
  label,
  title,
  disabled,
  onClick,
  icon,
}: {
  label: string
  title: string
  disabled: boolean
  onClick: () => void
  icon: ReactNode
}) {
  return (
    <button
      type="button"
      className="icon-button"
      aria-label={label}
      title={title}
      disabled={disabled}
      onClick={onClick}
    >
      {icon}
    </button>
  )
}
