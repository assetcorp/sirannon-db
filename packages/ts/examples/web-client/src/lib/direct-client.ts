import type { RemoteSubscription } from '@delali/sirannon-db/client'
import { SirannonClient } from '@delali/sirannon-db/client'
import type { CDCEvent } from './cdc'
import type {
  ActivityRecord,
  AddProductInput,
  InventorySnapshot,
  Product,
  ProductActionInput,
  ReceiveInventoryInput,
} from './schemas'
import { inventorySnapshotSchema } from './schemas'
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
  toWebSocketAuthProtocol,
} from './sql'

const DATA_ENDPOINT = import.meta.env.VITE_SIRANNON_ENDPOINT ?? DEFAULT_DATA_ENDPOINT
const DEMO_TOKEN = import.meta.env.VITE_SIRANNON_DEMO_TOKEN ?? DEFAULT_DEMO_TOKEN
const WEBSOCKET_AUTH_PROTOCOL = toWebSocketAuthProtocol(DEMO_TOKEN)

const httpClient = new SirannonClient(DATA_ENDPOINT, {
  transport: 'http',
  headers: {
    Authorization: `Bearer ${DEMO_TOKEN}`,
  },
})

const wsClient = new SirannonClient(DATA_ENDPOINT, {
  transport: 'websocket',
  webSocketProtocols: [WEBSOCKET_AUTH_PROTOCOL],
})

const httpDb = httpClient.database(DATABASE_ID)
const wsDb = wsClient.database(DATABASE_ID)

export async function fetchDirectSnapshot(): Promise<InventorySnapshot> {
  const [products, activity] = await Promise.all([
    httpDb.query<Product>(PRODUCT_LIST_SQL),
    httpDb.query<ActivityRecord>(ACTIVITY_LIST_SQL),
  ])
  return inventorySnapshotSchema.parse({ products, activity })
}

export async function allocateProductDirect(input: ProductActionInput): Promise<void> {
  await httpDb.transaction([
    {
      sql: ALLOCATE_PRODUCT_SQL,
      params: [input.id],
    },
    {
      sql: INSERT_ALLOCATE_ACTIVITY_SQL,
      params: [input.id],
    },
  ])
}

export async function receiveInventoryDirect(input: ReceiveInventoryInput): Promise<void> {
  await httpDb.transaction([
    {
      sql: RECEIVE_PRODUCT_SQL,
      params: [input.quantity, input.id],
    },
    {
      sql: INSERT_RECEIVE_ACTIVITY_SQL,
      params: [input.quantity, input.id],
    },
  ])
}

export async function addProductDirect(input: AddProductInput): Promise<void> {
  await httpDb.transaction([
    {
      sql: INSERT_PRODUCT_SQL,
      params: [input.name, input.price, input.stock],
    },
    {
      sql: INSERT_CREATED_ACTIVITY_SQL,
    },
  ])
}

export async function resetDatabaseDirect(): Promise<void> {
  await httpDb.transaction([
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
}

export async function subscribeProducts(callback: (event: CDCEvent) => void): Promise<RemoteSubscription> {
  return wsDb.on('products').subscribe(callback)
}

export async function subscribeActivity(callback: (event: CDCEvent) => void): Promise<RemoteSubscription> {
  return wsDb.on('activity').subscribe(callback)
}
