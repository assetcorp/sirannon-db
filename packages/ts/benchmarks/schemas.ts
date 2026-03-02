import { randomBytes } from 'node:crypto'

export const ycsbSchema = `
CREATE TABLE IF NOT EXISTS usertable (
  ycsb_key TEXT PRIMARY KEY,
  field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT,
  field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT
)
`

export const microSchemaSqlite = `
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  age INTEGER NOT NULL,
  bio TEXT
)
`

export const microSchemaPostgres = `
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  age INTEGER NOT NULL,
  bio TEXT
)
`

export const tpccSchemaSqlite = `
CREATE TABLE IF NOT EXISTS customers (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  balance REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  stock INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  total REAL NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  price REAL NOT NULL
);
`

export const tpccSchemaPostgres = `
CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  balance REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  stock INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  total REAL NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
  id SERIAL PRIMARY KEY,
  order_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  price REAL NOT NULL
);
`

function randomString(length: number): string {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  const bytes = randomBytes(length)
  let result = ''
  for (let i = 0; i < length; i++) {
    result += chars[bytes[i] % chars.length]
  }
  return result
}

export function generateYcsbRow(key: string): unknown[] {
  const fields: string[] = []
  for (let i = 0; i < 10; i++) {
    fields.push(randomString(100))
  }
  return [key, ...fields]
}

export function generateUserRow(id: number): unknown[] {
  const firstNames = ['Alice', 'Bob', 'Carol', 'Dave', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy']
  const lastNames = [
    'Smith',
    'Johnson',
    'Williams',
    'Brown',
    'Jones',
    'Garcia',
    'Miller',
    'Davis',
    'Rodriguez',
    'Martinez',
  ]
  const name = `${firstNames[id % firstNames.length]} ${lastNames[id % lastNames.length]}`
  const email = `user${id}@example.com`
  const age = 18 + (id % 62)
  const bio = `Bio for user ${id}: ${randomString(50)}`
  return [id, name, email, age, bio]
}

export function generateCustomer(id: number): unknown[] {
  const name = `Customer ${id}`
  const email = `customer${id}@store.com`
  const balance = 10000
  return [id, name, email, balance]
}

export function generateProduct(id: number): unknown[] {
  const name = `Product ${id}`
  const price = Math.round((10 + Math.random() * 490) * 100) / 100
  const stock = 1000
  return [id, name, price, stock]
}

/**
 * YCSB-compatible Zipfian generator (Gray et al., SIGMOD 1994).
 * Precomputes zeta constants at construction for O(1) sampling.
 */
export class ZipfianGenerator {
  private readonly items: number
  private readonly theta: number
  private readonly zetaN: number
  private readonly eta: number
  private readonly alpha: number

  constructor(items: number, theta = 0.99) {
    this.items = items
    this.theta = theta

    this.zetaN = this.computeZeta(items, theta)
    const zeta2 = this.computeZeta(2, theta)

    this.alpha = 1.0 / (1.0 - theta)
    this.eta = (1.0 - (2.0 / items) ** (1.0 - theta)) / (1.0 - zeta2 / this.zetaN)
  }

  next(): number {
    const u = Math.random()
    const uz = u * this.zetaN

    if (uz < 1.0) return 0
    if (uz < 1.0 + 0.5 ** this.theta) return 1

    return Math.floor(this.items * (this.eta * u - this.eta + 1.0) ** this.alpha)
  }

  private computeZeta(n: number, theta: number): number {
    let sum = 0
    for (let i = 0; i < n; i++) {
      sum += 1.0 / (i + 1) ** theta
    }
    return sum
  }
}
