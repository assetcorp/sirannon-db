import type { HTMLAttributes, ReactNode } from 'react'
import { cn } from '../../../../lib/ui'

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  children: ReactNode
}

export function Card({ children, className, ...props }: CardProps) {
  return (
    <section className={cn('ui-card', className)} {...props}>
      {children}
    </section>
  )
}

export function CardHeader({ children, className, ...props }: CardProps) {
  return (
    <div className={cn('ui-card-header', className)} {...props}>
      {children}
    </div>
  )
}

export function CardTitle({ children, className, ...props }: CardProps) {
  return (
    <div className={cn('ui-card-title', className)} {...props}>
      {children}
    </div>
  )
}

export function CardDescription({ children, className, ...props }: CardProps) {
  return (
    <div className={cn('ui-card-description', className)} {...props}>
      {children}
    </div>
  )
}

export function CardContent({ children, className, ...props }: CardProps) {
  return (
    <div className={cn('ui-card-content', className)} {...props}>
      {children}
    </div>
  )
}
