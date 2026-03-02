import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { type ComparisonPair, runComparison } from '../runner'
import { generateCustomer, generateProduct, tpccSchemaPostgres, tpccSchemaSqlite, ZipfianGenerator } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const FRAMING =
  'Mixed OLTP workload: New Order (45%), Payment (43%), Order Status (12%). ' +
  'Tests multi-statement transactions, lock management, and join performance. ' +
  'Single-client only; does not test concurrent transaction isolation or deadlock handling.'

const NUM_CUSTOMERS = 1_000
const NUM_PRODUCTS = 100
const NEW_ORDER_THRESHOLD = 0.45
const PAYMENT_THRESHOLD = 0.88

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping TPC-C Lite benchmark.')
    process.exit(0)
  }

  const systemInfo = collectSystemInfo()

  const sirannonEngine = createSirannonEngine(config)
  const postgresEngine = createPostgresEngine(config)

  await sirannonEngine.setup(tpccSchemaSqlite)
  await postgresEngine.setup(tpccSchemaPostgres)

  const customerRows = Array.from({ length: NUM_CUSTOMERS }, (_, i) => generateCustomer(i + 1))
  const productRows = Array.from({ length: NUM_PRODUCTS }, (_, i) => generateProduct(i + 1))

  await sirannonEngine.seed('INSERT INTO customers (id, name, email, balance) VALUES (?, ?, ?, ?)', customerRows)
  await sirannonEngine.seed('INSERT INTO products (id, name, price, stock) VALUES (?, ?, ?, ?)', productRows)

  await postgresEngine.seed('INSERT INTO customers (id, name, email, balance) VALUES ($1, $2, $3, $4)', customerRows)
  await postgresEngine.seed('INSERT INTO products (id, name, price, stock) VALUES ($1, $2, $3, $4)', productRows)

  const pgInfo = await postgresEngine.getInfo()
  systemInfo.postgresVersion = pgInfo.version ?? ''

  const db = sirannonEngine.db
  const pool = postgresEngine.pool

  const customerZipfian = new ZipfianGenerator(NUM_CUSTOMERS)
  const productZipfian = new ZipfianGenerator(NUM_PRODUCTS)

  let sirannonOrderId = 1
  let postgresOrderId = 100_000

  await pool.query({ name: 'tpc-select-customer', text: 'SELECT * FROM customers WHERE id = $1', values: [1] })
  await pool.query({ name: 'tpc-select-product', text: 'SELECT * FROM products WHERE id = $1', values: [1] })
  await pool.query({
    name: 'tpc-update-stock',
    text: 'UPDATE products SET stock = stock - $1 WHERE id = $2',
    values: [1, 1],
  })
  await pool.query({
    name: 'tpc-insert-order',
    text: 'INSERT INTO orders (id, customer_id, total, status, created_at) VALUES ($1, $2, $3, $4, $5)',
    values: [999999, 1, 0, 'pending', new Date().toISOString()],
  })
  await pool.query({
    name: 'tpc-insert-item',
    text: 'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)',
    values: [999999, 1, 1, 10],
  })
  await pool.query({
    name: 'tpc-update-balance-debit',
    text: 'UPDATE customers SET balance = balance - $1 WHERE id = $2',
    values: [0, 1],
  })
  await pool.query({
    name: 'tpc-update-balance-credit',
    text: 'UPDATE customers SET balance = balance + $1 WHERE id = $2',
    values: [0, 1],
  })
  await pool.query({
    name: 'tpc-order-status',
    text: `SELECT o.id, o.total, o.status, o.created_at, oi.product_id, oi.quantity, oi.price
           FROM orders o
           LEFT JOIN order_items oi ON oi.order_id = o.id
           WHERE o.customer_id = $1
           ORDER BY o.id DESC
           LIMIT 5`,
    values: [1],
  })
  await pool.query('DELETE FROM order_items WHERE order_id = 999999')
  await pool.query('DELETE FROM orders WHERE id = 999999')

  const pairs: ComparisonPair[] = [
    {
      workload: 'tpc-c-lite',
      dataSize: NUM_CUSTOMERS,
      framing: FRAMING,
      sirannon: {
        name: 'tpc-c-lite',
        fn: () => {
          const roll = Math.random()
          const customerId = (customerZipfian.next() % NUM_CUSTOMERS) + 1

          if (roll < NEW_ORDER_THRESHOLD) {
            const orderId = sirannonOrderId++
            const numItems = 1 + Math.floor(Math.random() * 5)
            db.transaction(tx => {
              tx.query('SELECT * FROM customers WHERE id = ?', [customerId])
              let total = 0
              for (let i = 0; i < numItems; i++) {
                const productId = (productZipfian.next() % NUM_PRODUCTS) + 1
                const quantity = 1 + Math.floor(Math.random() * 5)
                const product = tx.query<{ price: number; stock: number }>('SELECT * FROM products WHERE id = ?', [
                  productId,
                ])[0]
                if (product && product.stock >= quantity) {
                  tx.execute('UPDATE products SET stock = stock - ? WHERE id = ?', [quantity, productId])
                  tx.execute('INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)', [
                    orderId,
                    productId,
                    quantity,
                    product.price,
                  ])
                  total += product.price * quantity
                }
              }
              tx.execute(
                "INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (?, ?, ?, 'completed', ?)",
                [orderId, customerId, total, new Date().toISOString()],
              )
              tx.execute('UPDATE customers SET balance = balance - ? WHERE id = ?', [total, customerId])
            })
          } else if (roll < PAYMENT_THRESHOLD) {
            const paymentAmount = Math.round(Math.random() * 500 * 100) / 100
            db.transaction(tx => {
              tx.query('SELECT * FROM customers WHERE id = ?', [customerId])
              tx.execute('UPDATE customers SET balance = balance + ? WHERE id = ?', [paymentAmount, customerId])
            })
          } else {
            db.query(
              `SELECT o.id, o.total, o.status, o.created_at, oi.product_id, oi.quantity, oi.price
               FROM orders o
               LEFT JOIN order_items oi ON oi.order_id = o.id
               WHERE o.customer_id = ?
               ORDER BY o.id DESC
               LIMIT 5`,
              [customerId],
            )
          }
        },
        opts: { async: false },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: 'tpc-c-lite',
        fn: async () => {
          const roll = Math.random()
          const customerId = (customerZipfian.next() % NUM_CUSTOMERS) + 1

          if (roll < NEW_ORDER_THRESHOLD) {
            const orderId = postgresOrderId++
            const numItems = 1 + Math.floor(Math.random() * 5)
            const client = await pool.connect()
            try {
              await client.query('BEGIN')
              await client.query({
                name: 'tpc-select-customer',
                text: 'SELECT * FROM customers WHERE id = $1',
                values: [customerId],
              })

              let total = 0
              for (let i = 0; i < numItems; i++) {
                const productId = (productZipfian.next() % NUM_PRODUCTS) + 1
                const quantity = 1 + Math.floor(Math.random() * 5)
                const { rows } = await client.query({
                  name: 'tpc-select-product',
                  text: 'SELECT * FROM products WHERE id = $1',
                  values: [productId],
                })
                const product = rows[0]
                if (product && product.stock >= quantity) {
                  await client.query({
                    name: 'tpc-update-stock',
                    text: 'UPDATE products SET stock = stock - $1 WHERE id = $2',
                    values: [quantity, productId],
                  })
                  await client.query({
                    name: 'tpc-insert-item',
                    text: 'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)',
                    values: [orderId, productId, quantity, product.price],
                  })
                  total += product.price * quantity
                }
              }

              await client.query({
                name: 'tpc-insert-order',
                text: 'INSERT INTO orders (id, customer_id, total, status, created_at) VALUES ($1, $2, $3, $4, $5)',
                values: [orderId, customerId, total, 'completed', new Date().toISOString()],
              })
              await client.query({
                name: 'tpc-update-balance-debit',
                text: 'UPDATE customers SET balance = balance - $1 WHERE id = $2',
                values: [total, customerId],
              })
              await client.query('COMMIT')
            } catch (err) {
              await client.query('ROLLBACK')
              throw err
            } finally {
              client.release()
            }
          } else if (roll < PAYMENT_THRESHOLD) {
            const paymentAmount = Math.round(Math.random() * 500 * 100) / 100
            const client = await pool.connect()
            try {
              await client.query('BEGIN')
              await client.query({
                name: 'tpc-select-customer',
                text: 'SELECT * FROM customers WHERE id = $1',
                values: [customerId],
              })
              await client.query({
                name: 'tpc-update-balance-credit',
                text: 'UPDATE customers SET balance = balance + $1 WHERE id = $2',
                values: [paymentAmount, customerId],
              })
              await client.query('COMMIT')
            } catch (err) {
              await client.query('ROLLBACK')
              throw err
            } finally {
              client.release()
            }
          } else {
            await pool.query({
              name: 'tpc-order-status',
              text: `SELECT o.id, o.total, o.status, o.created_at, oi.product_id, oi.quantity, oi.price
           FROM orders o
           LEFT JOIN order_items oi ON oi.order_id = o.id
           WHERE o.customer_id = $1
           ORDER BY o.id DESC
           LIMIT 5`,
              values: [customerId],
            })
          }
        },
        opts: { async: true },
        afterAll: async () => {
          await postgresEngine.cleanup()
        },
      },
    },
  ]

  const results = await runComparison({ category: 'oltp-tpc-c-lite', ...config }, pairs)
  writeResults('oltp-tpc-c-lite', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
