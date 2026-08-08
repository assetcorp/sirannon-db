import type { LiveQuery } from '@delali/sirannon-db'
import type { SnapshotOutcome, SnapshotProgress } from '@delali/sirannon-db/client'
import {
  claimWorkOrder,
  completeWorkOrder,
  createWorkOrder,
  deviceNameFromLocation,
  type FieldDevice,
  openFieldDevice,
  openWorkOrders,
  reopenWorkOrder,
} from './device'
import { renderBanner, renderLink, renderOrders, renderSnapshot, renderSyncError, renderSyncStatus } from './render'
import type { WorkOrder } from './schema'
import { mountThemeToggle } from './theme'

const SERVER_URL = import.meta.env.VITE_SIRANNON_URL ?? 'http://127.0.0.1:9876'
const STATUS_INTERVAL_MS = 700

function required<T extends HTMLElement>(id: string): T {
  const node = document.getElementById(id)
  if (node === null) {
    throw new Error(`The page is missing the element '${id}'`)
  }
  return node as T
}

const deviceLabel = required('device-label')
const linkPill = required('link-pill')
const linkButton = required<HTMLButtonElement>('link-button')
const banner = required('banner')
const statusStrip = required('status-strip')
const syncError = required('sync-error')
const snapshotPanel = required('snapshot-panel')
const orderList = required('order-list')
const newOrderForm = required<HTMLFormElement>('new-order-form')
const siteInput = required<HTMLInputElement>('new-order-site')
const taskInput = required<HTMLInputElement>('new-order-task')

mountThemeToggle(required('theme-toggle'))

let device: FieldDevice | null = null
let orders: LiveQuery<WorkOrder> | null = null
let stopOrders: (() => void) | null = null
let changesReceived = 0
let wantsOnline = true

function report(err: unknown): void {
  renderBanner(banner, err instanceof Error ? err.message : String(err))
}

function paintOrders(): void {
  if (device === null || orders === null) return
  renderOrders(orderList, orders.getState(), device.name)
}

async function openOrderList(): Promise<void> {
  if (device === null || orders !== null) return
  const query = await openWorkOrders(device.db)
  orders = query
  stopOrders = query.subscribe(paintOrders)
  paintOrders()
}

async function closeOrderList(): Promise<void> {
  stopOrders?.()
  stopOrders = null
  const query = orders
  orders = null
  await query?.close()
}

function onServerChange(): void {
  changesReceived += 1
}

function onResyncRequired(): void {
  void closeOrderList()
}

function onSnapshotProgress(progress: SnapshotProgress): void {
  renderSnapshot(snapshotPanel, progress)
}

function onSnapshotComplete(outcome: SnapshotOutcome): void {
  renderSnapshot(snapshotPanel, null)
  if (outcome.databaseUsable) {
    void openOrderList()
  }
  if (!outcome.ok) {
    renderBanner(banner, `${outcome.error.message}${outcome.retrying ? ' Retrying shortly.' : ''}`)
  }
}

async function refreshStatus(): Promise<void> {
  if (device === null) return
  const status = await device.sync.status()
  renderSyncStatus(statusStrip, status, changesReceived)
  renderSyncError(syncError, status.lastError)
  renderLink(linkPill, linkButton, status, wantsOnline)
}

async function goOnline(): Promise<void> {
  if (device === null) return
  const status = await device.sync.status()
  if (status.state === 'paused') {
    await device.sync.resume()
    return
  }
  if (status.state === 'stopped') {
    await device.sync.start()
  }
}

async function toggleLink(): Promise<void> {
  if (device === null) return
  linkButton.disabled = true
  try {
    if (wantsOnline) {
      device.sync.pause()
      wantsOnline = false
    } else {
      await goOnline()
      wantsOnline = true
    }
    renderBanner(banner, null)
  } catch (err) {
    report(err)
  } finally {
    linkButton.disabled = false
    await refreshStatus()
  }
}

async function submitNewOrder(event: SubmitEvent): Promise<void> {
  event.preventDefault()
  if (device === null) return

  const site = siteInput.value.trim()
  const task = taskInput.value.trim()
  if (site.length === 0 || task.length === 0) return

  try {
    await createWorkOrder(device, site, task)
    newOrderForm.reset()
    siteInput.focus()
    renderBanner(banner, null)
  } catch (err) {
    report(err)
  }
  await refreshStatus()
}

async function runOrderAction(event: MouseEvent): Promise<void> {
  if (device === null) return

  const button = (event.target as HTMLElement).closest<HTMLButtonElement>('button[data-action]')
  if (button === null) return

  const card = button.closest<HTMLElement>('.order')
  const id = card?.dataset.id
  if (card === undefined || card === null || id === undefined) return

  const action = button.dataset.action
  try {
    if (action === 'claim') {
      await claimWorkOrder(device, id)
    } else if (action === 'complete') {
      const note = card.querySelector<HTMLInputElement>('input[data-role="note"]')?.value.trim() ?? ''
      await completeWorkOrder(device, id, note)
    } else if (action === 'reopen') {
      await reopenWorkOrder(device, id)
    }
    renderBanner(banner, null)
  } catch (err) {
    report(err)
  }
  await refreshStatus()
}

function onOrderListClick(event: MouseEvent): void {
  void runOrderAction(event)
}

function onNewOrderSubmit(event: SubmitEvent): void {
  void submitNewOrder(event)
}

function onLinkButtonClick(): void {
  void toggleLink()
}

async function start(): Promise<void> {
  const name = deviceNameFromLocation(window.location.search)
  deviceLabel.textContent = `Device ${name}, holding its own SQLite database in IndexedDB. Add ?device=<name> to the URL in another tab to run a second device against the same server.`

  device = await openFieldDevice(name, SERVER_URL, {
    onServerChange,
    onResyncRequired,
    onSnapshotProgress,
    onSnapshotComplete,
  })

  await openOrderList()

  try {
    await device.sync.start()
    if (device.neverSynced) {
      await closeOrderList()
      await device.sync.downloadSnapshot()
    }
  } catch (err) {
    wantsOnline = false
    report(err)
  }

  await openOrderList()
  await refreshStatus()

  orderList.addEventListener('click', onOrderListClick)
  newOrderForm.addEventListener('submit', onNewOrderSubmit)
  linkButton.addEventListener('click', onLinkButtonClick)
  window.setInterval(() => {
    void refreshStatus()
  }, STATUS_INTERVAL_MS)
}

start().catch(report)
