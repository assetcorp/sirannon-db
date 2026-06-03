import { disposeActivity, initActivity, refreshActivityFeed } from './activity.js'
import { resetDatabase } from './api.js'
import { disposeProducts, initProducts, refreshProducts } from './products.js'

const statusEl = document.getElementById('connection-status') as HTMLSpanElement
const resetBtn = document.getElementById('btn-reset') as HTMLButtonElement
const productsContainer = document.getElementById('products') as HTMLElement
const activityContainer = document.getElementById('activity') as HTMLElement

async function init(): Promise<void> {
  statusEl.textContent = 'Connecting...'
  statusEl.className = 'status-indicator connecting'

  try {
    await Promise.all([initProducts(productsContainer), initActivity(activityContainer)])

    statusEl.textContent = 'Connected'
    statusEl.className = 'status-indicator connected'
  } catch (err) {
    statusEl.textContent = 'Disconnected'
    statusEl.className = 'status-indicator disconnected'
    console.error('Failed to initialize:', err)
  }
}

async function handleResetClick(): Promise<void> {
  resetBtn.disabled = true
  try {
    await resetDatabase()
    await Promise.all([refreshProducts(), refreshActivityFeed()])
  } catch (err) {
    console.error('Reset failed:', err)
  } finally {
    resetBtn.disabled = false
  }
}

function dispose(): void {
  disposeProducts()
  disposeActivity()
}

type HotContext = {
  dispose(callback: () => void): void
}

type ImportMetaWithHot = ImportMeta & {
  hot?: HotContext
}

resetBtn.addEventListener('click', handleResetClick)

const hot = (import.meta as ImportMetaWithHot).hot
hot?.dispose(dispose)

init()
