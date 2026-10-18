import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { describeStreamedCopies } from './streamed-suite.js'

const EVERY_TEST_CONFIG_SETS_SQLITE_USE_URI = true

describeStreamedCopies('better-sqlite3', betterSqlite3, EVERY_TEST_CONFIG_SETS_SQLITE_USE_URI)
