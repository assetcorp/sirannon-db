import type { CDCEvent, Product } from './api.js'
import { addProduct, fetchProducts, restockProduct, sellProduct, subscribeProducts } from './api.js'

const products = new Map<number, Product>()

let tableBody: HTMLTableSectionElement
let form: HTMLFormElement

function escapeHtml(text: string): string {
  const el = document.createElement('span')
  el.textContent = text
  return el.innerHTML
}

function formatPrice(price: number): string {
  return `$${price.toFixed(2)}`
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

  const sellBtn = row.querySelector('.btn-sell') as HTMLButtonElement
  const restockBtn = row.querySelector('.btn-restock') as HTMLButtonElement

  sellBtn.addEventListener('click', async () => {
    sellBtn.disabled = true
    try {
      await sellProduct(product.id, product.name)
    } catch (err) {
      console.error('Sell failed:', err)
      sellBtn.disabled = product.stock <= 1
    }
  })

  restockBtn.addEventListener('click', async () => {
    restockBtn.disabled = true
    try {
      await restockProduct(product.id, product.name, 10)
    } catch (err) {
      console.error('Restock failed:', err)
    } finally {
      restockBtn.disabled = false
    }
  })

  return row
}

function updateRow(product: Product): void {
  const row = tableBody.querySelector(`tr[data-id="${product.id}"]`)
  if (!row) {
    tableBody.appendChild(createRow(product))
    return
  }

  const cells = row.querySelectorAll('td')
  cells[1].textContent = product.name
  cells[2].textContent = formatPrice(product.price)
  cells[3].textContent = String(product.stock)

  const sellBtn = row.querySelector('.btn-sell') as HTMLButtonElement
  sellBtn.disabled = product.stock <= 0
}

export function clearProductsTable(): void {
  products.clear()
  if (tableBody) {
    tableBody.innerHTML = ''
  }
}

function removeRow(id: number): void {
  const row = tableBody.querySelector(`tr[data-id="${id}"]`)
  if (row) {
    row.remove()
  }
}

function handleCDCEvent(event: CDCEvent): void {
  const row = event.row as unknown as Product

  if (event.type === 'delete') {
    products.delete(row.id)
    removeRow(row.id)
    return
  }

  products.set(row.id, row)
  updateRow(row)
}

function setupForm(): void {
  form.addEventListener('submit', async e => {
    e.preventDefault()

    const nameInput = form.querySelector<HTMLInputElement>('[name="product-name"]')
    const priceInput = form.querySelector<HTMLInputElement>('[name="product-price"]')
    const stockInput = form.querySelector<HTMLInputElement>('[name="product-stock"]')

    if (!nameInput || !priceInput || !stockInput) return

    const name = nameInput.value.trim()
    const price = parseFloat(priceInput.value)
    const stock = parseInt(stockInput.value, 10)

    if (!name || Number.isNaN(price) || Number.isNaN(stock) || price <= 0 || stock < 0) return

    const submitBtn = form.querySelector<HTMLButtonElement>('button[type="submit"]')
    if (submitBtn) submitBtn.disabled = true

    try {
      await addProduct(name, price, stock)
      form.reset()
    } catch (err) {
      console.error('Add product failed:', err)
    } finally {
      if (submitBtn) submitBtn.disabled = false
    }
  })
}

export async function initProducts(container: HTMLElement): Promise<void> {
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

  tableBody = container.querySelector('tbody') as HTMLTableSectionElement
  form = container.querySelector('form') as HTMLFormElement
  setupForm()

  const initialProducts = await fetchProducts()
  for (const product of initialProducts) {
    products.set(product.id, product)
    tableBody.appendChild(createRow(product))
  }

  await subscribeProducts(handleCDCEvent)
}
