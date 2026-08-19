import { nodeSqlite } from '../../../drivers/node/index.js'
import { describeStreamedBackup } from './streamed-backup-suite.js'

describeStreamedBackup('node:sqlite', nodeSqlite)
