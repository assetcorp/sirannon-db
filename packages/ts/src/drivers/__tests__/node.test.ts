import { nodeSqlite } from '../node/index.js'
import { runConformanceTests } from './conformance.js'

runConformanceTests(() => nodeSqlite(), 'node:sqlite')
