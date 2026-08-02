import type { LiveQueryState } from '@delali/sirannon-db'
import type { SnapshotProgress, SyncState, SyncStatus } from '@delali/sirannon-db/client'
import type { WorkOrder, WorkOrderStatus } from './schema'

const STATUS_LABELS: Record<WorkOrderStatus, string> = {
  scheduled: 'Scheduled',
  in_progress: 'In progress',
  done: 'Done',
}

function element<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  className?: string,
  text?: string,
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag)
  if (className !== undefined) node.className = className
  if (text !== undefined) node.textContent = text
  return node
}

function actionButton(action: string, label: string): HTMLButtonElement {
  const button = element('button', 'action', label)
  button.type = 'button'
  button.dataset.action = action
  return button
}

function orderCard(order: WorkOrder, deviceName: string): HTMLElement {
  const card = element('article', 'order')
  card.dataset.id = order.id

  const head = element('div', 'order-head')
  head.append(element('h3', undefined, order.site))
  head.append(element('span', `pill pill-${order.status}`, STATUS_LABELS[order.status]))
  card.append(head)

  card.append(element('p', 'order-task', order.task))

  const held = order.technician.length > 0
  const heldByThisDevice = held && order.technician === deviceName
  const meta = held ? `Held by ${order.technician}${heldByThisDevice ? ' (this device)' : ''}` : 'Unassigned'
  card.append(element('p', 'order-meta', meta))

  if (order.note.length > 0) {
    card.append(element('p', 'order-note', order.note))
  }

  const actions = element('div', 'order-actions')
  if (order.status === 'scheduled') {
    actions.append(actionButton('claim', 'Claim'))
  }
  if (order.status === 'in_progress') {
    const note = element('input', 'note-input')
    note.type = 'text'
    note.placeholder = 'What did you do?'
    note.dataset.role = 'note'
    actions.append(note)
    actions.append(actionButton('complete', 'Mark done'))
  }
  if (order.status === 'done') {
    actions.append(actionButton('reopen', 'Reopen'))
  }
  card.append(actions)

  return card
}

export function renderOrders(container: HTMLElement, state: LiveQueryState<WorkOrder>, deviceName: string): void {
  container.replaceChildren()

  if (state.status === 'pending') {
    container.append(element('p', 'empty', 'Opening the local work order list...'))
    return
  }

  if (state.status === 'error') {
    container.append(element('p', 'empty', `The local work order list failed: ${state.error.message}`))
    return
  }

  if (state.rows.length === 0) {
    container.append(element('p', 'empty', 'No work orders on this device yet. Add one below.'))
    return
  }

  for (const order of state.rows) {
    container.append(orderCard(order, deviceName))
  }

  if (state.revalidating) {
    container.append(element('p', 'empty', 'Re-reading after a wide change...'))
  }
}

export function renderSyncStatus(container: HTMLElement, status: SyncStatus, changesReceived: number): void {
  const entries: [string, string][] = [
    ['Sync state', status.state],
    ['Waiting to push', String(status.pendingPushCount)],
    ['Pulled through', status.lastPulledSeq === null ? 'nothing yet' : status.lastPulledSeq.toString()],
    ['Changes from server', String(changesReceived)],
    ['Schema version', status.schemaVersion === null ? 'unknown' : String(status.schemaVersion)],
  ]

  container.replaceChildren()
  for (const [label, value] of entries) {
    const cell = element('div', 'stat')
    cell.append(element('span', 'stat-label', label))
    cell.append(element('span', 'stat-value', value))
    container.append(cell)
  }
}

export function renderSyncError(line: HTMLElement, lastError: SyncStatus['lastError']): void {
  line.hidden = lastError === null
  line.textContent = lastError === null ? '' : `Last sync error, ${lastError.code}: ${lastError.message}`
}

export function renderSnapshot(panel: HTMLElement, progress: SnapshotProgress | null): void {
  if (progress === null) {
    panel.hidden = true
    panel.replaceChildren()
    return
  }

  const share = progress.totalRows === 0 ? 1 : progress.loadedRows / progress.totalRows
  panel.hidden = false
  panel.replaceChildren()
  panel.append(
    element(
      'p',
      undefined,
      `Loading a fresh copy from the server: ${progress.loadedRows} of ${progress.totalRows} rows`,
    ),
  )
  const track = element('div', 'progress-track')
  const bar = element('div', 'progress-bar')
  bar.style.width = `${Math.round(share * 100)}%`
  track.append(bar)
  panel.append(track)
}

export function renderBanner(banner: HTMLElement, message: string | null): void {
  banner.hidden = message === null
  banner.textContent = message ?? ''
}

const LINK_LABELS: Record<SyncState, string> = {
  stopped: 'Disconnected',
  starting: 'Connecting',
  running: 'Online',
  paused: 'Offline',
  snapshotting: 'Loading a copy',
}

export function renderLink(
  pill: HTMLElement,
  button: HTMLButtonElement,
  status: SyncStatus,
  wantsOnline: boolean,
): void {
  const stalled = status.resyncRequired
  pill.textContent = stalled ? 'Needs a fresh copy' : LINK_LABELS[status.state]
  pill.className = `pill pill-link-${stalled ? 'resync' : status.state}`
  button.textContent = wantsOnline ? 'Go offline' : 'Come online'
}
