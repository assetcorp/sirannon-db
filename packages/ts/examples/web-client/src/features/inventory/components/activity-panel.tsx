import { Activity } from 'lucide-react'
import type { ActivityRecord } from '../../../lib/schemas'
import { activityLabel, formatTimestamp } from '../inventory-utils'
import { PanelHeader } from './panel-header'

export function ActivityPanel({ records }: { records: ActivityRecord[] }) {
  const items = records.map(record => <ActivityItem key={record.id} record={record} />)

  return (
    <aside className="activity-panel">
      <PanelHeader icon={<Activity size={18} />} title="Change Log" />
      <ol className="activity-list">{items}</ol>
    </aside>
  )
}

function ActivityItem({ record }: { record: ActivityRecord }) {
  return (
    <li className={`activity-item ${record.action}`}>
      <span className="activity-dot" />
      <div>
        <strong>{activityLabel(record)}</strong>
        <span>{formatTimestamp(record.created_at)}</span>
      </div>
    </li>
  )
}
