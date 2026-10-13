import { nodeSqlite } from '../../../drivers/node/index.js'
import { nodeSqliteParsesBackupUris } from '../../helpers/streaming-extension.js'
import { describeStreamedBackpressure, describeStreamedCopies } from './streamed-suite.js'

describeStreamedCopies('node:sqlite', nodeSqlite, nodeSqliteParsesBackupUris())
describeStreamedBackpressure('node:sqlite', nodeSqlite)
