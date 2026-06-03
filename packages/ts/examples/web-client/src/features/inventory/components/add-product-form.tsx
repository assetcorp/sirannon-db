import { PackagePlus } from 'lucide-react'
import type { ChangeEvent, FormEvent } from 'react'
import { useCallback, useState } from 'react'
import type { AddProductInput } from '../../../lib/schemas'
import { EMPTY_FORM, type ProductFormState } from '../types'

export function AddProductForm({
  disabled,
  onSubmit,
}: {
  disabled: boolean
  onSubmit: (input: AddProductInput) => Promise<boolean>
}) {
  const [form, setForm] = useState<ProductFormState>(EMPTY_FORM)

  const handleNameChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    const { value } = event.currentTarget
    setForm(current => ({ ...current, name: value }))
  }, [])

  const handlePriceChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    const { value } = event.currentTarget
    setForm(current => ({ ...current, price: value }))
  }, [])

  const handleStockChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    const { value } = event.currentTarget
    setForm(current => ({ ...current, stock: value }))
  }, [])

  const handleSubmit = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()

      const name = form.name.trim()
      const price = Number.parseFloat(form.price)
      const stock = Number.parseInt(form.stock, 10)

      if (!name || !Number.isFinite(price) || !Number.isSafeInteger(stock) || price <= 0 || stock < 0) {
        return
      }

      const saved = await onSubmit({ name, price, stock })
      if (saved) {
        setForm(EMPTY_FORM)
      }
    },
    [form, onSubmit],
  )

  return (
    <form className="add-product-form" autoComplete="off" onSubmit={handleSubmit}>
      <label>
        <span>Item</span>
        <input
          type="text"
          name="product-name"
          placeholder="SKU or item name"
          value={form.name}
          disabled={disabled}
          onChange={handleNameChange}
        />
      </label>
      <label>
        <span>Unit cost</span>
        <input
          type="number"
          name="product-price"
          min="0.01"
          step="0.01"
          value={form.price}
          disabled={disabled}
          onChange={handlePriceChange}
        />
      </label>
      <label>
        <span>On hand</span>
        <input
          type="number"
          name="product-stock"
          min="0"
          step="1"
          value={form.stock}
          disabled={disabled}
          onChange={handleStockChange}
        />
      </label>
      <button type="submit" disabled={disabled}>
        <PackagePlus size={16} />
        Create
      </button>
    </form>
  )
}
