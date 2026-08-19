import { describe, expect, it } from 'vitest'
import { describeBackupCapabilities } from '../../backup/capabilities.js'

describe('describeBackupCapabilities', () => {
  it('states that a runtime with a stepped copy needs local disk equal to the backup', () => {
    const report = describeBackupCapabilities({ multipleConnections: true, extensions: true, steppedCopy: true }, true)

    expect(report).toEqual({
      fullCopy: true,
      streamedCopy: false,
      stagedCopy: true,
      localDiskRequired: 'equal-to-backup',
      schedule: true,
    })
  })

  it('reports no full copy where the runtime carries no stepped copy call', () => {
    const report = describeBackupCapabilities(
      { multipleConnections: false, extensions: false, steppedCopy: false },
      true,
    )

    expect(report.fullCopy).toBe(false)
    expect(report.stagedCopy).toBe(false)
    expect(report.localDiskRequired).toBe('none')
    expect(report.schedule).toBe(false)
  })

  it('states that a runtime streaming its copy needs no local disk', () => {
    const report = describeBackupCapabilities(
      { multipleConnections: true, extensions: true, steppedCopy: true },
      true,
      true,
    )

    expect(report).toEqual({
      fullCopy: true,
      streamedCopy: true,
      stagedCopy: true,
      localDiskRequired: 'none',
      schedule: true,
    })
  })

  it('reports no streamed copy where the runtime carries no stepped copy call', () => {
    const report = describeBackupCapabilities(
      { multipleConnections: false, extensions: false, steppedCopy: false },
      true,
      true,
    )

    expect(report.streamedCopy).toBe(false)
    expect(report.localDiskRequired).toBe('none')
  })

  it('reports nothing at all where the driver supplies no backup engine', () => {
    const report = describeBackupCapabilities({ multipleConnections: true, extensions: true, steppedCopy: true }, false)

    expect(report.fullCopy).toBe(false)
    expect(report.schedule).toBe(false)
  })
})
