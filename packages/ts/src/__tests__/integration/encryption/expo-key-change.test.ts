import { beforeEach, describe, vi } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { expoSqlcipherBuild } from '../../helpers/expo-modules.js'
import { readWithIndependentSqlcipher } from '../../helpers/independent-sqlcipher.js'
import { builtExpoSqlcipherBindings } from '../../helpers/sirannon-sqlite.js'
import { describeConnectionsAfterKeyChange } from './connections-after-key-change-suite.js'
import { describeKeyChange } from './key-change-suite.js'

vi.mock('expo-sqlite', async () => (await import('../../helpers/expo-modules.js')).expoSqliteModule())
vi.mock('expo-file-system/legacy', async () =>
  (await import('../../helpers/expo-modules.js')).expoFileSystemLegacyModule(),
)

const EXPO_DRIVER_MODULE = '../../../drivers/expo/index.js'
const { expoSqlite } = (await import(EXPO_DRIVER_MODULE)) as { expoSqlite: () => SQLiteDriver }

for (const { version, binding } of builtExpoSqlcipherBindings()) {
  describe.skipIf(binding === null)(`the Expo driver over the SQLCipher of expo-sqlite ${version}`, () => {
    beforeEach(() => {
      expoSqlcipherBuild.binding = binding
    })

    const newSirannon = () => new Sirannon({ driver: expoSqlite() })
    describeKeyChange(`the Expo driver over expo-sqlite ${version}`, newSirannon, readWithIndependentSqlcipher)
    describeConnectionsAfterKeyChange(`the Expo driver over expo-sqlite ${version}`, newSirannon)
  })
}
