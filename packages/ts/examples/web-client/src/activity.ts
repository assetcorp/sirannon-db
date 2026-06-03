import type { RemoteSubscription } from '@delali/sirannon-db/client'
import type { ActivityRecord, CDCEvent } from './api.js'
import { fetchRecentActivity, subscribeActivity } from './api.js'

let feedList: HTMLUListElement | null = null
let subscription: RemoteSubscription | null = null
const displayedIds = new Set<number>()

function escapeHtml(text: string): string {
  const el = document.createElement('span')
  el.textContent = text
  return el.innerHTML
}

function formatAction(record: ActivityRecord): string {
  const labels: Record<string, string> = {
    sale: 'Sold',
    restock: 'Restocked',
    added: 'Added',
  }
  const label = labels[record.action] ?? record.action
  return `${label} ${record.quantity}x ${escapeHtml(record.product_name)}`
}

function formatTimestamp(iso: string): string {
  const date = new Date(`${iso}Z`)
  return date.toLocaleTimeString()
}

function toSafeInteger(value: unknown): number | null {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN
  return Number.isSafeInteger(parsed) ? parsed : null
}

function normaliseActivityRecord(row: Record<string, unknown> | undefined): ActivityRecord | null {
  const id = toSafeInteger(row?.id)
  const quantity = toSafeInteger(row?.quantity)
  const productName = row?.product_name
  const action = row?.action
  const createdAt = row?.created_at

  if (
    id === null ||
    quantity === null ||
    typeof productName !== 'string' ||
    (action !== 'sale' && action !== 'restock' && action !== 'added') ||
    typeof createdAt !== 'string'
  ) {
    return null
  }

  return {
    id,
    product_name: productName,
    action,
    quantity,
    created_at: createdAt,
  }
}

function createEntry(record: ActivityRecord): HTMLLIElement {
  const li = document.createElement('li')
  li.dataset.id = String(record.id)
  const safeAction = /^[a-z]+$/.test(record.action) ? record.action : 'unknown'
  li.className = `activity-entry activity-${safeAction}`
  li.innerHTML = `
    <span class="activity-text">${formatAction(record)}</span>
    <span class="activity-time">${formatTimestamp(record.created_at)}</span>
  `
  return li
}

function handleCDCEvent(event: CDCEvent): void {
  if (!feedList) return

  if (event.type === 'delete') {
    const record = normaliseActivityRecord(event.oldRow ?? event.row)
    if (!record) return

    displayedIds.delete(record.id)
    feedList.querySelector(`li[data-id="${record.id}"]`)?.remove()
    return
  }

  if (event.type !== 'insert') return

  const record = normaliseActivityRecord(event.row)
  if (!record) return
  if (displayedIds.has(record.id)) return

  displayedIds.add(record.id)
  const entry = createEntry(record)
  feedList.prepend(entry)
}

export function clearActivityFeed(): void {
  displayedIds.clear()
  if (feedList) {
    feedList.innerHTML = ''
  }
}

function renderActivity(records: ActivityRecord[]): void {
  displayedIds.clear()
  if (!feedList) return

  feedList.innerHTML = ''
  for (const record of records) {
    displayedIds.add(record.id)
    feedList.appendChild(createEntry(record))
  }
}

export async function refreshActivityFeed(): Promise<void> {
  const recentActivity = await fetchRecentActivity()
  renderActivity(recentActivity)
}

export function disposeActivity(): void {
  subscription?.unsubscribe()
  subscription = null
}

export async function initActivity(container: HTMLElement): Promise<void> {
  disposeActivity()
  displayedIds.clear()

  container.innerHTML = `
    <h3>Activity Feed</h3>
    <ul class="activity-list"></ul>
  `

  feedList = container.querySelector<HTMLUListElement>('.activity-list')

  await refreshActivityFeed()

  subscription = await subscribeActivity(handleCDCEvent)
}
