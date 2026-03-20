import { clearActivityFeed, initActivity } from './activity.js'
import { resetDatabase } from './api.js'
import { clearProductsTable, initProducts } from './products.js'

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

resetBtn.addEventListener('click', async () => {
  resetBtn.disabled = true
  try {
    clearActivityFeed()
    clearProductsTable()
    await resetDatabase()
  } catch (err) {
    console.error('Reset failed:', err)
  } finally {
    resetBtn.disabled = false
  }
})

init()
