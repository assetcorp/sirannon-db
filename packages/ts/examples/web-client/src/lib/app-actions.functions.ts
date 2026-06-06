import type { RemoteDatabase } from '@delali/sirannon-db/client'
import { SirannonClient } from '@delali/sirannon-db/client'
import { createServerFn, createServerOnlyFn } from '@tanstack/react-start'
import type { ActivityRecord, Product } from './schemas'
import {
  addProductInputSchema,
  inventorySnapshotSchema,
  productIdInputSchema,
  receiveInventoryInputSchema,
} from './schemas'
import {
  ACTIVITY_LIST_SQL,
  ALLOCATE_PRODUCT_SQL,
  DATABASE_ID,
  DEFAULT_DATA_ENDPOINT,
  DEFAULT_DEMO_TOKEN,
  DELETE_ACTIVITY_SQL,
  DELETE_PRODUCTS_SQL,
  INSERT_ALLOCATE_ACTIVITY_SQL,
  INSERT_CREATED_ACTIVITY_SQL,
  INSERT_PRODUCT_SQL,
  INSERT_RECEIVE_ACTIVITY_SQL,
  PRODUCT_LIST_SQL,
  RECEIVE_PRODUCT_SQL,
  RESET_SEQUENCE_SQL,
  SEED_PRODUCTS,
} from './sql'

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

export const getInventorySnapshot = createServerFn({
  method: 'GET',
}).handler(async () => {
  const db = getServerHttpDb()
  const [products, activity] = await Promise.all([
    db.query<Product>(PRODUCT_LIST_SQL),
    db.query<ActivityRecord>(ACTIVITY_LIST_SQL),
  ])
  return inventorySnapshotSchema.parse({ products, activity })
})

export const allocateProduct = createServerFn({
  method: 'POST',
})
  .inputValidator(data => productIdInputSchema.parse(data))
  .handler(async ({ data }) => {
    const db = getServerHttpDb()
    await db.transaction([
      {
        sql: ALLOCATE_PRODUCT_SQL,
        params: [data.id],
      },
      {
        sql: INSERT_ALLOCATE_ACTIVITY_SQL,
        params: [data.id],
      },
    ])
  })

export const receiveInventory = createServerFn({
  method: 'POST',
})
  .inputValidator(data => receiveInventoryInputSchema.parse(data))
  .handler(async ({ data }) => {
    const db = getServerHttpDb()
    await db.transaction([
      {
        sql: RECEIVE_PRODUCT_SQL,
        params: [data.quantity, data.id],
      },
      {
        sql: INSERT_RECEIVE_ACTIVITY_SQL,
        params: [data.quantity, data.id],
      },
    ])
  })

export const addProduct = createServerFn({
  method: 'POST',
})
  .inputValidator(data => addProductInputSchema.parse(data))
  .handler(async ({ data }) => {
    const db = getServerHttpDb()
    await db.transaction([
      {
        sql: INSERT_PRODUCT_SQL,
        params: [data.name, data.price, data.stock],
      },
      {
        sql: INSERT_CREATED_ACTIVITY_SQL,
      },
    ])
  })

export const resetDatabase = createServerFn({
  method: 'POST',
}).handler(async () => {
  const db = getServerHttpDb()
  await db.transaction([
    { sql: DELETE_ACTIVITY_SQL },
    { sql: DELETE_PRODUCTS_SQL },
    {
      sql: RESET_SEQUENCE_SQL,
      params: ['activity', 'products'],
    },
    ...SEED_PRODUCTS.map(product => ({
      sql: INSERT_PRODUCT_SQL,
      params: [product.name, product.price, product.stock],
    })),
  ])
})
