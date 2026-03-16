import { betterSqlite3 } from '../better-sqlite3/index.js'
import { runConformanceTests } from './conformance.js'

runConformanceTests(() => betterSqlite3(), 'better-sqlite3')
