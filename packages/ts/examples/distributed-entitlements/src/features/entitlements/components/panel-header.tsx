import type { ReactNode } from 'react'

export function PanelHeader({ icon, title, action }: { icon: ReactNode; title: string; action?: ReactNode }) {
  return (
    <div className="panel-header">
      <div className="panel-title">
        <span className="panel-icon">{icon}</span>
        <h2>{title}</h2>
      </div>
      {action ? <div className="panel-action">{action}</div> : null}
    </div>
  )
}
