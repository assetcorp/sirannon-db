import type { RemoteSubscription } from '@delali/sirannon-db/client'
import { SirannonClient } from '@delali/sirannon-db/client'

const HTTP_URL = 'http://localhost:9876'
const WS_URL = 'ws://localhost:9876'

const httpClient = new SirannonClient(HTTP_URL, { transport: 'http' })
const wsClient = new SirannonClient(WS_URL, { transport: 'websocket' })

const httpDb = httpClient.database('main')
const wsDb = wsClient.database('main')

export interface Product {
  id: number
  name: string
  price: number
  stock: number
}

export interface ActivityRecord {
  id: number
  product_name: string
  action: 'sale' | 'restock' | 'added'
  quantity: number
  created_at: string
}

export interface CDCEvent {
  type: 'insert' | 'update' | 'delete'
  table: string
  row: Record<string, unknown>
  oldRow?: Record<string, unknown>
  seq: bigint
  timestamp: number
}

export async function fetchProducts(): Promise<Product[]> {
  return httpDb.query<Product>('SELECT * FROM products ORDER BY id')
}

export async function fetchRecentActivity(): Promise<ActivityRecord[]> {
  return httpDb.query<ActivityRecord>('SELECT * FROM activity ORDER BY id DESC LIMIT 20')
}

export async function sellProduct(id: number, name: string): Promise<void> {
  await httpDb.transaction([
    {
      sql: 'UPDATE products SET stock = stock - 1 WHERE id = ? AND stock > 0',
      params: [id],
    },
    {
      sql: 'INSERT INTO activity (product_name, action, quantity) VALUES (?, ?, ?)',
      params: [name, 'sale', 1],
    },
  ])
}

export async function restockProduct(id: number, name: string, qty: number): Promise<void> {
  await httpDb.transaction([
    {
      sql: 'UPDATE products SET stock = stock + ? WHERE id = ?',
      params: [qty, id],
    },
    {
      sql: 'INSERT INTO activity (product_name, action, quantity) VALUES (?, ?, ?)',
      params: [name, 'restock', qty],
    },
  ])
}

export async function addProduct(name: string, price: number, stock: number): Promise<void> {
  await httpDb.transaction([
    {
      sql: 'INSERT INTO products (name, price, stock) VALUES (?, ?, ?)',
      params: [name, price, stock],
    },
    {
      sql: 'INSERT INTO activity (product_name, action, quantity) VALUES (?, ?, ?)',
      params: [name, 'added', stock],
    },
  ])
}

export async function resetDatabase(): Promise<void> {
  await httpDb.transaction([
    { sql: 'DELETE FROM activity' },
    { sql: 'DELETE FROM products' },
    {
      sql: 'INSERT INTO products (name, price, stock) VALUES (?, ?, ?)',
      params: ['Wireless Keyboard', 49.99, 25],
    },
    {
      sql: 'INSERT INTO products (name, price, stock) VALUES (?, ?, ?)',
      params: ['USB-C Cable', 12.99, 150],
    },
    {
      sql: 'INSERT INTO products (name, price, stock) VALUES (?, ?, ?)',
      params: ['27" Monitor', 349.99, 8],
    },
    {
      sql: 'INSERT INTO products (name, price, stock) VALUES (?, ?, ?)',
      params: ['Mechanical Keyboard', 89.99, 42],
    },
    {
      sql: 'INSERT INTO products (name, price, stock) VALUES (?, ?, ?)',
      params: ['Webcam HD', 59.99, 30],
    },
  ])
}

export async function subscribeProducts(callback: (event: CDCEvent) => void): Promise<RemoteSubscription> {
  return wsDb.on('products').subscribe(callback)
}

export async function subscribeActivity(callback: (event: CDCEvent) => void): Promise<RemoteSubscription> {
  return wsDb.on('activity').subscribe(callback)
}
