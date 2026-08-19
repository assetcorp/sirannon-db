import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { describeStreamedBackup } from './streamed-backup-suite.js'

describeStreamedBackup('better-sqlite3', betterSqlite3)
