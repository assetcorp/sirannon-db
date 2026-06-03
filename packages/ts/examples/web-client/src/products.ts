import type { RemoteSubscription } from '@delali/sirannon-db/client'
import type { CDCEvent, Product } from './api.js'
import { addProduct, fetchProducts, restockProduct, sellProduct, subscribeProducts } from './api.js'

const products = new Map<number, Product>()

let tableBody: HTMLTableSectionElement | null = null
let form: HTMLFormElement | null = null
let subscription: RemoteSubscription | null = null

function escapeHtml(text: string): string {
  const el = document.createElement('span')
  el.textContent = text
  return el.innerHTML
}

function formatPrice(price: number): string {
  return `$${price.toFixed(2)}`
}

function toSafeInteger(value: unknown): number | null {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN
  return Number.isSafeInteger(parsed) ? parsed : null
}

function toFiniteNumber(value: unknown): number | null {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN
  return Number.isFinite(parsed) ? parsed : null
}

function normaliseProduct(row: Record<string, unknown> | undefined): Product | null {
  const id = toSafeInteger(row?.id)
  const price = toFiniteNumber(row?.price)
  const stock = toSafeInteger(row?.stock)
  const name = row?.name

  if (id === null || price === null || stock === null || typeof name !== 'string') {
    return null
  }

  return { id, name, price, stock }
}

function createRow(product: Product): HTMLTableRowElement {
  const row = document.createElement('tr')
  row.dataset.id = String(product.id)

  const sellDisabled = product.stock <= 0 ? 'disabled' : ''

  row.innerHTML = `
    <td>${product.id}</td>
    <td>${escapeHtml(product.name)}</td>
    <td>${formatPrice(product.price)}</td>
    <td class="stock-cell">${product.stock}</td>
    <td class="actions-cell">
      <button type="button" class="btn-sell" ${sellDisabled}>Sell</button>
      <button type="button" class="btn-restock">Restock</button>
    </td>
  `

  const sellBtn = row.querySelector<HTMLButtonElement>('.btn-sell')
  const restockBtn = row.querySelector<HTMLButtonElement>('.btn-restock')

  sellBtn?.addEventListener('click', async () => {
    sellBtn.disabled = true
    try {
      const current = products.get(product.id) ?? product
      await sellProduct(product.id, current.name)
    } catch (err) {
      console.error('Sell failed:', err)
      const current = products.get(product.id) ?? product
      sellBtn.disabled = current.stock <= 0
    }
  })

  restockBtn?.addEventListener('click', async () => {
    restockBtn.disabled = true
    try {
      const current = products.get(product.id) ?? product
      await restockProduct(product.id, current.name, 10)
    } catch (err) {
      console.error('Restock failed:', err)
    } finally {
      restockBtn.disabled = false
    }
  })

  return row
}

function updateRow(product: Product): void {
  if (!tableBody) return

  const row = tableBody.querySelector(`tr[data-id="${product.id}"]`)
  if (!row) {
    tableBody.appendChild(createRow(product))
    return
  }

  const cells = row.querySelectorAll('td')
  const nameCell = cells.item(1)
  const priceCell = cells.item(2)
  const stockCell = cells.item(3)
  if (nameCell) nameCell.textContent = product.name
  if (priceCell) priceCell.textContent = formatPrice(product.price)
  if (stockCell) stockCell.textContent = String(product.stock)

  const sellBtn = row.querySelector<HTMLButtonElement>('.btn-sell')
  if (sellBtn) sellBtn.disabled = product.stock <= 0
}

export function clearProductsTable(): void {
  products.clear()
  if (tableBody) {
    tableBody.innerHTML = ''
  }
}

function removeRow(id: number): void {
  if (!tableBody) return

  const row = tableBody.querySelector(`tr[data-id="${id}"]`)
  if (row) {
    row.remove()
  }
}

function handleCDCEvent(event: CDCEvent): void {
  if (event.type === 'delete') {
    const product = normaliseProduct(event.oldRow ?? event.row)
    if (!product) return

    products.delete(product.id)
    removeRow(product.id)
    return
  }

  const product = normaliseProduct(event.row)
  if (!product) return

  products.set(product.id, product)
  updateRow(product)
}

function setupForm(): void {
  if (!form) return

  const activeForm = form

  activeForm.addEventListener('submit', async e => {
    e.preventDefault()

    const nameInput = activeForm.querySelector<HTMLInputElement>('[name="product-name"]')
    const priceInput = activeForm.querySelector<HTMLInputElement>('[name="product-price"]')
    const stockInput = activeForm.querySelector<HTMLInputElement>('[name="product-stock"]')

    if (!nameInput || !priceInput || !stockInput) return

    const name = nameInput.value.trim()
    const price = parseFloat(priceInput.value)
    const stock = parseInt(stockInput.value, 10)

    if (!name || Number.isNaN(price) || Number.isNaN(stock) || price <= 0 || stock < 0) return

    const submitBtn = activeForm.querySelector<HTMLButtonElement>('button[type="submit"]')
    if (submitBtn) submitBtn.disabled = true

    try {
      await addProduct(name, price, stock)
      activeForm.reset()
    } catch (err) {
      console.error('Add product failed:', err)
    } finally {
      if (submitBtn) submitBtn.disabled = false
    }
  })
}

function renderProducts(nextProducts: Product[]): void {
  products.clear()
  if (!tableBody) return

  tableBody.innerHTML = ''
  for (const product of nextProducts) {
    products.set(product.id, product)
    tableBody.appendChild(createRow(product))
  }
}

export async function refreshProducts(): Promise<void> {
  const nextProducts = await fetchProducts()
  renderProducts(nextProducts)
}

export function disposeProducts(): void {
  subscription?.unsubscribe()
  subscription = null
}

export async function initProducts(container: HTMLElement): Promise<void> {
  disposeProducts()
  products.clear()

  container.innerHTML = `
    <form class="add-product-form" autocomplete="off">
      <h3>Add Product</h3>
      <div class="form-row">
        <input type="text" name="product-name" placeholder="Product name" required />
        <input type="number" name="product-price" placeholder="Price" step="0.01" min="0.01" required />
        <input type="number" name="product-stock" placeholder="Stock" min="0" required />
        <button type="submit">Add</button>
      </div>
    </form>
    <table class="product-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Name</th>
          <th>Price</th>
          <th>Stock</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  `

  tableBody = container.querySelector<HTMLTableSectionElement>('tbody')
  form = container.querySelector<HTMLFormElement>('form')
  setupForm()

  await refreshProducts()

  subscription = await subscribeProducts(handleCDCEvent)
}
