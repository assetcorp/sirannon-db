import { nodeSqliteParsesBackupUris } from '../../../__tests__/helpers/streaming-extension.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { describeStreamedBackup, describeStreamedBackupBackpressure } from './streamed-backup-suite.js'

describeStreamedBackup('node:sqlite', nodeSqlite, nodeSqliteParsesBackupUris())
describeStreamedBackupBackpressure('node:sqlite', nodeSqlite)
