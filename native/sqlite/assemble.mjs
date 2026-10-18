import { copyBetterSqliteJavaScript } from './better-sqlite3-source.mjs'

try {
  console.log(`Copied better-sqlite3's JavaScript to ${copyBetterSqliteJavaScript()}`)
} catch (err) {
  console.error(`Assembling @delali/sirannon-sqlite failed: ${err.message}`)
  process.exit(1)
}
