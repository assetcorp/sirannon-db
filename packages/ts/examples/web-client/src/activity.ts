import type { ActivityRecord, CDCEvent } from './api.js'
import { fetchRecentActivity, subscribeActivity } from './api.js'

let feedList: HTMLUListElement
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

function createEntry(record: ActivityRecord): HTMLLIElement {
  const li = document.createElement('li')
  const safeAction = /^[a-z]+$/.test(record.action) ? record.action : 'unknown'
  li.className = `activity-entry activity-${safeAction}`
  li.innerHTML = `
    <span class="activity-text">${formatAction(record)}</span>
    <span class="activity-time">${formatTimestamp(record.created_at)}</span>
  `
  return li
}

function handleCDCEvent(event: CDCEvent): void {
  if (event.type !== 'insert') return

  const record = event.row as unknown as ActivityRecord
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

export async function initActivity(container: HTMLElement): Promise<void> {
  container.innerHTML = `
    <h3>Activity Feed</h3>
    <ul class="activity-list"></ul>
  `

  feedList = container.querySelector('.activity-list') as HTMLUListElement

  const recentActivity = await fetchRecentActivity()
  for (const record of recentActivity) {
    displayedIds.add(record.id)
    feedList.appendChild(createEntry(record))
  }

  await subscribeActivity(handleCDCEvent)
}
