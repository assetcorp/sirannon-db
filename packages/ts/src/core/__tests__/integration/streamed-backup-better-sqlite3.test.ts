import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { describeStreamedBackup } from './streamed-backup-suite.js'

const EVERY_TEST_CONFIG_SETS_SQLITE_USE_URI = true

describeStreamedBackup('better-sqlite3', betterSqlite3, EVERY_TEST_CONFIG_SETS_SQLITE_USE_URI)
