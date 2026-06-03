export const DATABASE_ID = 'main'
export const DEFAULT_DATA_ENDPOINT = 'http://localhost:9876'
export const DEFAULT_DEMO_TOKEN = 'sirannon-demo-token'
export const WEBSOCKET_AUTH_PROTOCOL_PREFIX = 'sirannon.demo.auth.'

export const PRODUCT_LIST_SQL = 'SELECT * FROM products ORDER BY id'
export const ACTIVITY_LIST_SQL = 'SELECT * FROM activity ORDER BY id DESC LIMIT 20'

export const ALLOCATE_PRODUCT_SQL = 'UPDATE products SET stock = stock - 1 WHERE id = ? AND stock > 0'
export const RECEIVE_PRODUCT_SQL = 'UPDATE products SET stock = stock + ? WHERE id = ?'
export const INSERT_PRODUCT_SQL = 'INSERT INTO products (name, price, stock) VALUES (?, ?, ?)'
export const INSERT_ACTIVITY_SQL = 'INSERT INTO activity (product_name, action, quantity) VALUES (?, ?, ?)'
export const DELETE_ACTIVITY_SQL = 'DELETE FROM activity'
export const DELETE_PRODUCTS_SQL = 'DELETE FROM products'
export const RESET_SEQUENCE_SQL = 'DELETE FROM sqlite_sequence WHERE name IN (?, ?)'

export const SEED_PRODUCTS = [
  { name: 'Edge Gateway AX-140', price: 219.5, stock: 38 },
  { name: 'Industrial Sensor Module S-22', price: 84.25, stock: 126 },
  { name: 'Rack Power Controller RP-9', price: 410, stock: 12 },
  { name: 'Field Service Tablet T-8', price: 575, stock: 18 },
  { name: 'Secure Access Badge Pack', price: 32.75, stock: 240 },
] as const

export function toWebSocketAuthProtocol(token: string): string {
  const bytes = new TextEncoder().encode(token)
  let binary = ''

  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }

  const encoded = btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
  return `${WEBSOCKET_AUTH_PROTOCOL_PREFIX}${encoded}`
}
