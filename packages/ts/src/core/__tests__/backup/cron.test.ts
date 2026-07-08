import { describe, expect, it } from 'vitest'
import { parseCron } from '../../backup/cron.js'

function at(year: number, monthIndex: number, day: number, hours = 0, minutes = 0, seconds = 0): Date {
  return new Date(year, monthIndex, day, hours, minutes, seconds)
}

describe('parseCron', () => {
  describe('field matching', () => {
    it('matches every minute for the wildcard expression', () => {
      const cron = parseCron('* * * * *')
      expect(cron.hasSeconds).toBe(false)
      expect(cron.matches(at(2023, 5, 15, 3, 7))).toBe(true)
      expect(cron.matches(at(2023, 11, 31, 23, 59))).toBe(true)
    })

    it('matches a fixed minute and hour only', () => {
      const cron = parseCron('30 9 * * *')
      expect(cron.matches(at(2023, 0, 1, 9, 30))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 9, 31))).toBe(false)
      expect(cron.matches(at(2023, 0, 1, 10, 30))).toBe(false)
    })

    it('honours step values', () => {
      const cron = parseCron('*/15 * * * *')
      expect(cron.matches(at(2023, 0, 1, 0, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 0, 15))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 0, 45))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 0, 20))).toBe(false)
    })

    it('honours ranges with a step', () => {
      const cron = parseCron('0 9-17/4 * * *')
      expect(cron.matches(at(2023, 0, 1, 9, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 13, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 17, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 10, 0))).toBe(false)
    })

    it('honours comma-separated lists', () => {
      const cron = parseCron('0 0,12 * * *')
      expect(cron.matches(at(2023, 0, 1, 0, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 12, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 6, 0))).toBe(false)
    })

    it('resolves month and weekday names', () => {
      const cron = parseCron('0 0 * JAN MON')
      expect(cron.matches(at(2023, 0, 2, 0, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 3, 0, 0))).toBe(false)
      expect(cron.matches(at(2023, 1, 6, 0, 0))).toBe(false)
    })

    it('treats weekday 7 as Sunday', () => {
      const cron = parseCron('0 0 * * 7')
      expect(cron.matches(at(2023, 0, 1, 0, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 2, 0, 0))).toBe(false)
    })

    it('matches a seconds field in six-field expressions', () => {
      const cron = parseCron('30 * * * * *')
      expect(cron.hasSeconds).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 0, 0, 30))).toBe(true)
      expect(cron.matches(at(2023, 0, 1, 0, 0, 31))).toBe(false)
    })
  })

  describe('day-of-month and day-of-week combining', () => {
    it('matches either field when both are restricted', () => {
      const cron = parseCron('0 0 13 * 5')
      expect(cron.matches(at(2023, 0, 13, 0, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 6, 0, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 14, 0, 0))).toBe(false)
    })

    it('requires the day-of-month when day-of-week is unrestricted', () => {
      const cron = parseCron('0 0 13 * *')
      expect(cron.matches(at(2023, 0, 13, 0, 0))).toBe(true)
      expect(cron.matches(at(2023, 0, 6, 0, 0))).toBe(false)
    })
  })

  describe('nicknames', () => {
    it('expands @daily', () => {
      const cron = parseCron('@daily')
      expect(cron.matches(at(2023, 5, 15, 0, 0))).toBe(true)
      expect(cron.matches(at(2023, 5, 15, 0, 1))).toBe(false)
    })

    it('expands @hourly', () => {
      const cron = parseCron('@hourly')
      expect(cron.matches(at(2023, 5, 15, 4, 0))).toBe(true)
      expect(cron.matches(at(2023, 5, 15, 4, 30))).toBe(false)
    })

    it('rejects an unknown nickname', () => {
      expect(() => parseCron('@reboot')).toThrow(/unsupported nickname/)
    })
  })

  describe('validation', () => {
    it('rejects an empty expression', () => {
      expect(() => parseCron('   ')).toThrow(/empty/)
    })

    it('rejects the wrong field count', () => {
      expect(() => parseCron('* * *')).toThrow(/expected 5 or 6 fields/)
    })

    it('rejects out-of-range values', () => {
      expect(() => parseCron('60 * * * *')).toThrow(/out of range/)
      expect(() => parseCron('0 24 * * *')).toThrow(/out of range/)
    })

    it('rejects an inverted range', () => {
      expect(() => parseCron('0 17-9 * * *')).toThrow(/after end/)
    })

    it('rejects a zero step', () => {
      expect(() => parseCron('*/0 * * * *')).toThrow(/step must be at least 1/)
    })

    it('rejects unknown names', () => {
      expect(() => parseCron('0 0 * FOO *')).toThrow(/unknown name/)
    })

    it('rejects unsupported Quartz-style syntax', () => {
      expect(() => parseCron('0 0 L * *')).toThrow(/unsupported syntax/)
      expect(() => parseCron('0 0 * * 5#2')).toThrow(/unsupported syntax/)
      expect(() => parseCron('0 0 15W * *')).toThrow(/unsupported syntax/)
      expect(() => parseCron('0 0 ? * *')).toThrow(/unsupported syntax/)
    })
  })
})
