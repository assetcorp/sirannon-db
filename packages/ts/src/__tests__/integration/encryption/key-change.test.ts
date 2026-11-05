import { describe } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { readWithIndependentAegis } from '../../helpers/independent-aegis.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'
import { describeKeyChange } from './key-change-suite.js'

const binding = builtSirannonSqliteBindingPath()

describe.skipIf(binding === null)('the better-sqlite3 driver', () => {
  describeKeyChange(
    'the better-sqlite3 driver',
    () => new Sirannon({ driver: betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined }) }),
    readWithIndependentAegis,
  )
})
