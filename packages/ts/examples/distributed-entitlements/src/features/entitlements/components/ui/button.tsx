import type { ButtonHTMLAttributes, ReactNode } from 'react'
import { cn } from '../../../../lib/ui'

type ButtonVariant = 'default' | 'secondary' | 'outline' | 'ghost' | 'destructive'
type ButtonSize = 'default' | 'sm' | 'icon'

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode
  variant?: ButtonVariant
  size?: ButtonSize
}

export function Button({ children, className, variant = 'default', size = 'default', ...props }: ButtonProps) {
  return (
    <button className={cn('ui-button', `ui-button-${variant}`, `ui-button-${size}`, className)} {...props}>
      {children}
    </button>
  )
}
