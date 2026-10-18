import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createWorkspace, seedOrders, type Workspace } from './harness.js'
import {
  advanceMinutes,
  advanceSeconds,
  letCopiesFinish,
  schedule,
  startScheduleClock,
  wallClock,
} from './schedule-harness.js'

let workspace: Workspace
let opened = 0

beforeEach(() => {
  workspace = createWorkspace('schedule-clock')
})

afterEach(async () => {
  vi.useRealTimers()
  await workspace.dispose()
})

interface Window {
  cron: string
  timezone: string
  startIso: string
  minutes: number
}

async function wallClockFires(window: Window): Promise<{ labels: string[]; epochs: number[] }> {
  opened++
  const db = await workspace.open(`clock-${opened}`)
  await seedOrders(db, 'clock', 3)
  startScheduleClock(window.startIso)
  const scheduled = schedule(db, {
    cron: window.cron,
    timezone: window.timezone,
    destDir: workspace.path(`copies-${opened}`),
  })
  await advanceMinutes(window.minutes)
  vi.useRealTimers()
  expect(scheduled.errors).toEqual([])
  const epochs = scheduled.reports.map(report => report.startedAt)
  return { labels: epochs.map(epoch => wallClock(epoch, window.timezone)), epochs }
}

describe('a schedule across daylight-saving changes', () => {
  it.each<[string, Window, string[]]>([
    [
      'New York, where the clocks go back an hour',
      { cron: '0,30 1 * * *', timezone: 'America/New_York', startIso: '2023-11-05T04:59:00Z', minutes: 96 },
      ['01:00', '01:30'],
    ],
    [
      'Lord Howe Island, where the clocks go back half an hour',
      { cron: '30,45 1 * * *', timezone: 'Australia/Lord_Howe', startIso: '2023-04-01T14:29:00Z', minutes: 51 },
      ['01:30', '01:45'],
    ],
    [
      'Sydney, where the clocks go back in April',
      { cron: '0,30 2 * * *', timezone: 'Australia/Sydney', startIso: '2023-04-01T14:59:00Z', minutes: 96 },
      ['02:00', '02:30'],
    ],
  ])('runs each time in the repeated hour once in %s', async (_label, window, expected) => {
    const { labels } = await wallClockFires(window)

    expect(labels).toEqual(expected)
  })

  it.each<[string, Window]>([
    [
      'New York, where the clocks go forward an hour',
      { cron: '0,30 2 * * *', timezone: 'America/New_York', startIso: '2023-03-12T06:29:00Z', minutes: 66 },
    ],
    [
      'Lord Howe Island, where the clocks go forward half an hour',
      { cron: '0,15 2 * * *', timezone: 'Australia/Lord_Howe', startIso: '2023-09-30T15:00:00Z', minutes: 90 },
    ],
    [
      'Sydney, where the clocks go forward in October',
      { cron: '0,30 2 * * *', timezone: 'Australia/Sydney', startIso: '2023-09-30T15:59:00Z', minutes: 66 },
    ],
  ])('skips the times the missing hour would have held in %s', async (_label, window) => {
    const { labels } = await wallClockFires(window)

    expect(labels).toEqual([])
  })
})

describe('a schedule in a named time zone', () => {
  it.each<[string, Window, string]>([
    [
      'Kolkata, half an hour off the hour',
      { cron: '0 9 * * *', timezone: 'Asia/Kolkata', startIso: '2023-06-01T03:29:00Z', minutes: 3 },
      '2023-06-01T03:30:00.000Z',
    ],
    [
      'UTC',
      { cron: '0 12 * * *', timezone: 'UTC', startIso: '2023-06-01T11:59:00Z', minutes: 3 },
      '2023-06-01T12:00:00.000Z',
    ],
    [
      'New York in summer',
      { cron: '0 12 * * *', timezone: 'America/New_York', startIso: '2023-07-03T15:59:00Z', minutes: 3 },
      '2023-07-03T16:00:00.000Z',
    ],
  ])('runs at the wall-clock time of %s', async (_label, window, instant) => {
    const { epochs } = await wallClockFires(window)

    expect(epochs.map(epoch => new Date(epoch).toISOString())).toEqual([instant])
  })
})

describe('the fields of a cron expression', () => {
  it.each<[string, string, number, string[]]>([
    ['0 0 * * 7', '2023-01-07T23:59:00Z', 2, ['2023-01-08T00:00:00']],
    ['0 0 * * 7', '2023-01-08T23:59:00Z', 2, []],
    ['0 0 * JAN MON', '2023-01-01T23:59:00Z', 2, ['2023-01-02T00:00:00']],
    ['0 0 * JAN MON', '2023-02-05T23:59:00Z', 2, []],
    ['0 0 13 * 5', '2023-01-05T23:59:00Z', 2, ['2023-01-06T00:00:00']],
    ['0 0 13 * 5', '2023-02-12T23:59:00Z', 2, ['2023-02-13T00:00:00']],
    ['0 0 13 * 5', '2023-01-13T23:59:00Z', 2, []],
    ['0 0 13 * *', '2023-01-05T23:59:00Z', 2, []],
    ['0 9-17/4 * * *', '2023-06-01T12:59:00Z', 2, ['2023-06-01T13:00:00']],
    ['0 9-17/4 * * *', '2023-06-01T10:59:00Z', 2, []],
    ['@hourly', '2023-06-01T03:59:00Z', 2, ['2023-06-01T04:00:00']],
    ['@daily', '2023-06-01T23:59:00Z', 2, ['2023-06-02T00:00:00']],
  ])('fires %s from %s only at the times it names', async (cron, startIso, minutes, expected) => {
    const { epochs } = await wallClockFires({ cron, timezone: 'UTC', startIso, minutes })

    expect(epochs.map(epoch => new Date(epoch).toISOString().slice(0, 19))).toEqual(expected)
  })

  it.each<[string, number[]]>([
    ['*/15 * * * * *', [15, 30, 45, 0]],
    ['10-20/5 * * * * *', [10, 15, 20]],
    ['0,30 * * * * *', [30, 0]],
  ])('fires %s at the seconds it names', async (cron, seconds) => {
    opened++
    const db = await workspace.open(`seconds-${opened}`)
    await seedOrders(db, 'seconds', 3)
    startScheduleClock('2023-06-01T10:00:00Z')
    const scheduled = schedule(db, { cron, timezone: 'UTC', destDir: workspace.path(`seconds-${opened}`) })

    await advanceSeconds(60)

    expect(scheduled.errors).toEqual([])
    expect(scheduled.reports.map(report => new Date(report.startedAt).getUTCSeconds())).toEqual(seconds)
  })
})

describe('a schedule whose clock jumps', () => {
  it('fills in none of the times a forward jump passes over', async () => {
    const db = await workspace.open('jumped')
    await seedOrders(db, 'jumped', 3)
    startScheduleClock('2023-06-01T00:59:00Z')
    const scheduled = schedule(db, { cron: '* * * * *', timezone: 'UTC', destDir: workspace.path('jumped-copies') })

    await advanceMinutes(2)
    const beforeJump = scheduled.reports.length
    expect(scheduled.errors).toEqual([])
    vi.setSystemTime(new Date('2023-06-01T04:00:00Z'))
    await advanceMinutes(2)

    const labels = scheduled.reports.map(report => wallClock(report.startedAt, 'UTC'))
    expect(beforeJump).toBe(2)
    expect(labels).toEqual(['01:00', '01:01', '04:01', '04:02'])
  })

  it('repeats nothing after the clock steps back until time passes the last copy again', async () => {
    const db = await workspace.open('rewound')
    await seedOrders(db, 'rewound', 3)
    startScheduleClock('2023-06-01T01:04:00Z')
    const scheduled = schedule(db, { cron: '* * * * *', timezone: 'UTC', destDir: workspace.path('rewound-copies') })

    await advanceMinutes(2)
    vi.setSystemTime(new Date('2023-06-01T01:00:00Z'))
    await advanceMinutes(5)
    await letCopiesFinish()
    const duringTheRewind = scheduled.reports.length
    await advanceMinutes(3)

    const labels = scheduled.reports.map(report => wallClock(report.startedAt, 'UTC'))
    expect(scheduled.errors).toEqual([])
    expect(labels.slice(0, 2)).toEqual(['01:05', '01:06'])
    expect(duringTheRewind).toBe(2)
    expect(labels.slice(2)).toEqual(['01:07', '01:08'])
  })
})
