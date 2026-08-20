import { nodeSqlite } from '../../../drivers/node/index.js'
import { describeStreamedBackup, describeStreamedBackupBackpressure } from './streamed-backup-suite.js'

describeStreamedBackup('node:sqlite', nodeSqlite)
describeStreamedBackupBackpressure('node:sqlite', nodeSqlite)
