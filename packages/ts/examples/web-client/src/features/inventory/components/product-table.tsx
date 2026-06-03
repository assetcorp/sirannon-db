import { PackageCheck, RefreshCw } from 'lucide-react'
import { useCallback } from 'react'
import type { Product } from '../../../lib/schemas'
import { formatPrice } from '../inventory-utils'

export function ProductTable({
  products,
  pendingAction,
  onAllocate,
  onReceive,
}: {
  products: Product[]
  pendingAction: string | null
  onAllocate: (product: Product) => Promise<void>
  onReceive: (product: Product) => Promise<void>
}) {
  const rows = products.map(product => (
    <ProductRow
      key={product.id}
      product={product}
      disabled={pendingAction !== null}
      onAllocate={onAllocate}
      onReceive={onReceive}
    />
  ))

  return (
    <div className="table-wrap">
      <table className="product-table">
        <thead>
          <tr>
            <th>Record</th>
            <th>Item</th>
            <th>Unit cost</th>
            <th>Available</th>
            <th>Workflow</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
  )
}

function ProductRow({
  product,
  disabled,
  onAllocate,
  onReceive,
}: {
  product: Product
  disabled: boolean
  onAllocate: (product: Product) => Promise<void>
  onReceive: (product: Product) => Promise<void>
}) {
  const handleAllocateClick = useCallback(() => {
    void onAllocate(product)
  }, [onAllocate, product])

  const handleReceiveClick = useCallback(() => {
    void onReceive(product)
  }, [onReceive, product])

  return (
    <tr>
      <td>{product.id}</td>
      <td>{product.name}</td>
      <td>{formatPrice(product.price)}</td>
      <td>
        <span className={product.stock <= 10 ? 'stock-count low' : 'stock-count'}>{product.stock}</span>
      </td>
      <td>
        <div className="row-actions">
          <button type="button" disabled={disabled || product.stock <= 0} onClick={handleAllocateClick}>
            <PackageCheck size={14} />
            Allocate
          </button>
          <button type="button" disabled={disabled} onClick={handleReceiveClick}>
            <RefreshCw size={14} />
            Receive +10
          </button>
        </div>
      </td>
    </tr>
  )
}
