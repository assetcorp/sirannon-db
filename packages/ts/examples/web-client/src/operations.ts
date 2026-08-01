import { type OperationRegistry, RequestDeniedError } from '@delali/sirannon-db'
import { DATABASE_ID } from './lib/demo-config'

export interface Operator {
  operatorId: string
}

export const SEED_PRODUCTS = [
  { name: 'Edge Gateway AX-140', price: 219.5, stock: 38 },
  { name: 'Industrial Sensor Module S-22', price: 84.25, stock: 126 },
  { name: 'Rack Power Controller RP-9', price: 410, stock: 12 },
  { name: 'Field Service Tablet T-8', price: 575, stock: 18 },
  { name: 'Secure Access Badge Pack', price: 32.75, stock: 240 },
] as const

export const MAX_PRODUCT_NAME_LENGTH = 80
export const MAX_PRODUCT_PRICE = 100_000
export const MAX_PRODUCT_STOCK = 100_000
export const MAX_RECEIVED_QUANTITY = 1_000
export const ACTIVITY_FEED_LIMIT = 20

const PRODUCT_LIST_SQL = 'SELECT id, name, price, stock FROM products ORDER BY id'

const ACTIVITY_LIST_SQL = `
  SELECT id, product_name, action, quantity, operator, created_at
  FROM activity
  ORDER BY id DESC
  LIMIT ${ACTIVITY_FEED_LIMIT}
`

const ALLOCATE_PRODUCT_SQL = 'UPDATE products SET stock = stock - 1 WHERE id = ? AND stock > 0'

const RECEIVE_PRODUCT_SQL = 'UPDATE products SET stock = stock + ? WHERE id = ?'

const INSERT_PRODUCT_SQL = 'INSERT INTO products (name, price, stock) VALUES (?, ?, ?)'

const LOG_ALLOCATION_SQL = `
  INSERT INTO activity (product_name, action, quantity, operator)
  SELECT name, 'allocated', 1, ?
  FROM products
  WHERE id = ? AND changes() > 0
`

const LOG_RECEIPT_SQL = `
  INSERT INTO activity (product_name, action, quantity, operator)
  SELECT name, 'received', ?, ?
  FROM products
  WHERE id = ? AND changes() > 0
`

const LOG_CREATION_SQL = `
  INSERT INTO activity (product_name, action, quantity, operator)
  SELECT name, 'created', stock, ?
  FROM products
  WHERE id = last_insert_rowid() AND changes() > 0
`

const DELETE_ACTIVITY_SQL = 'DELETE FROM activity'
const DELETE_PRODUCTS_SQL = 'DELETE FROM products'

function refuse(message: string): never {
  throw new RequestDeniedError(400, 'INVALID_ARGUMENT', message)
}

function readProductId(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1) {
    refuse('productId must be a positive integer')
  }
  return value
}

function readQuantity(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1 || value > MAX_RECEIVED_QUANTITY) {
    refuse(`quantity must be an integer between 1 and ${MAX_RECEIVED_QUANTITY}`)
  }
  return value
}

function readProductName(value: unknown): string {
  const name = typeof value === 'string' ? value.trim() : ''
  if (name.length === 0 || name.length > MAX_PRODUCT_NAME_LENGTH) {
    refuse(`name must be between 1 and ${MAX_PRODUCT_NAME_LENGTH} characters`)
  }
  return name
}

function readPrice(value: unknown): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0 || value > MAX_PRODUCT_PRICE) {
    refuse(`price must be greater than 0 and at most ${MAX_PRODUCT_PRICE}`)
  }
  return value
}

function readStock(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || value > MAX_PRODUCT_STOCK) {
    refuse(`stock must be an integer between 0 and ${MAX_PRODUCT_STOCK}`)
  }
  return value
}

function readOperator(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0) {
    refuse('operator is filled from the caller identity and was missing')
  }
  return value
}

export const operations = {
  [DATABASE_ID]: {
    reads: {
      products: {
        columns: ['id', 'name', 'price', 'stock'],
        statement: () => ({ sql: PRODUCT_LIST_SQL }),
      },
      activity: {
        columns: ['id', 'product_name', 'action', 'quantity', 'operator', 'created_at'],
        statement: () => ({ sql: ACTIVITY_LIST_SQL }),
      },
    },
    writes: {
      allocateProduct: {
        args: ['productId'],
        fromIdentity: { operator: 'operatorId' },
        statements: args => {
          const productId = readProductId(args.productId)
          const operator = readOperator(args.operator)
          return [
            { sql: ALLOCATE_PRODUCT_SQL, params: [productId] },
            { sql: LOG_ALLOCATION_SQL, params: [operator, productId] },
          ]
        },
      },
      receiveInventory: {
        args: ['productId', 'quantity'],
        fromIdentity: { operator: 'operatorId' },
        statements: args => {
          const productId = readProductId(args.productId)
          const quantity = readQuantity(args.quantity)
          const operator = readOperator(args.operator)
          return [
            { sql: RECEIVE_PRODUCT_SQL, params: [quantity, productId] },
            { sql: LOG_RECEIPT_SQL, params: [quantity, operator, productId] },
          ]
        },
      },
      addProduct: {
        args: ['name', 'price', 'stock'],
        fromIdentity: { operator: 'operatorId' },
        statements: args => {
          const name = readProductName(args.name)
          const price = readPrice(args.price)
          const stock = readStock(args.stock)
          const operator = readOperator(args.operator)
          return [
            { sql: INSERT_PRODUCT_SQL, params: [name, price, stock] },
            { sql: LOG_CREATION_SQL, params: [operator] },
          ]
        },
      },
      resetInventory: {
        statements: () => [
          { sql: DELETE_ACTIVITY_SQL },
          { sql: DELETE_PRODUCTS_SQL },
          ...SEED_PRODUCTS.map(product => ({
            sql: INSERT_PRODUCT_SQL,
            params: [product.name, product.price, product.stock],
          })),
        ],
      },
    },
  },
} satisfies OperationRegistry<Operator>
