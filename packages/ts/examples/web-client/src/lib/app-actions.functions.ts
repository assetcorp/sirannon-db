import type { RemoteDatabase } from '@delali/sirannon-db/client'
import { SirannonClient } from '@delali/sirannon-db/client'
import { createServerFn, createServerOnlyFn } from '@tanstack/react-start'
import { main } from '../generated/operations'
import { DATABASE_ID, DEFAULT_DATA_ENDPOINT, DEFAULT_DEMO_TOKEN } from './demo-config'
import { addProductInputSchema, productIdInputSchema, receiveInventoryInputSchema } from './schemas'

let cachedHttpDb: RemoteDatabase | null = null

const getServerHttpDb = createServerOnlyFn(() => {
  if (cachedHttpDb) {
    return cachedHttpDb
  }

  const endpoint = process.env.SIRANNON_ENDPOINT ?? DEFAULT_DATA_ENDPOINT
  const token = process.env.SIRANNON_DEMO_TOKEN ?? DEFAULT_DEMO_TOKEN
  const client = new SirannonClient(endpoint, {
    transport: 'http',
    headers: {
      Authorization: `Bearer ${token}`,
    },
  })

  cachedHttpDb = client.database(DATABASE_ID)
  return cachedHttpDb
})

export const allocateProduct = createServerFn({
  method: 'POST',
})
  .inputValidator(data => productIdInputSchema.parse(data))
  .handler(async ({ data }) => {
    await getServerHttpDb().execute(main.writes.allocateProduct, { productId: data.productId })
  })

export const receiveInventory = createServerFn({
  method: 'POST',
})
  .inputValidator(data => receiveInventoryInputSchema.parse(data))
  .handler(async ({ data }) => {
    await getServerHttpDb().execute(main.writes.receiveInventory, {
      productId: data.productId,
      quantity: data.quantity,
    })
  })

export const addProduct = createServerFn({
  method: 'POST',
})
  .inputValidator(data => addProductInputSchema.parse(data))
  .handler(async ({ data }) => {
    await getServerHttpDb().execute(main.writes.addProduct, {
      name: data.name,
      price: data.price,
      stock: data.stock,
    })
  })

export const resetInventory = createServerFn({
  method: 'POST',
}).handler(async () => {
  await getServerHttpDb().execute(main.writes.resetInventory, {})
})
