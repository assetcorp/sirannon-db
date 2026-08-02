import type { OperationRef } from '@delali/sirannon-db'

export const registryDigest = "325fedeefaa16097a9d65d744704b9f46b61448275fda41c1f5ceaf1461da8de"

export interface MainActivityRow {
  id: unknown
  product_name: unknown
  action: unknown
  quantity: unknown
  operator: unknown
  created_at: unknown
}

export interface MainProductsRow {
  id: unknown
  name: unknown
  price: unknown
  stock: unknown
}

export const main = {
  reads: {
    activity: { name: "activity" } as OperationRef<Record<string, never>, MainActivityRow>,
    products: { name: "products" } as OperationRef<Record<string, never>, MainProductsRow>,
  },
  writes: {
    addProduct: { name: "addProduct" } as OperationRef<{ name: unknown; price: unknown; stock: unknown }, never>,
    allocateProduct: { name: "allocateProduct" } as OperationRef<{ productId: unknown }, never>,
    receiveInventory: { name: "receiveInventory" } as OperationRef<{ productId: unknown; quantity: unknown }, never>,
    resetInventory: { name: "resetInventory" } as OperationRef<Record<string, never>, never>,
  },
}
