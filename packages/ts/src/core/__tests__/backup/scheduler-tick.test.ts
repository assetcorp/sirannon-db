import { describe, expect, it } from 'vitest'
import { parseCron } from '../../backup/cron.js'
import { evaluateTick, INITIAL_TICK_STATE, type TickState } from '../../backup/scheduler.js'

const everyMinute = parseCron('* * * * *')
const topOfHour = parseCron('0 * * * *')

function stateAfter(slot: string, epochIso: string): TickState {
  return { lastFiredSlot: slot, lastFiredEpoch: new Date(epochIso).getTime() }
}

describe('evaluateTick', () => {
  it('fires on the first evaluation from the initial state', () => {
    const result = evaluateTick(new Date('2023-06-01T00:00:00Z'), everyMinute, 'UTC', INITIAL_TICK_STATE)
    expect(result.shouldFire).toBe(true)
    expect(result.nextState.lastFiredEpoch).toBe(new Date('2023-06-01T00:00:00Z').getTime())
  })

  it('fires again on the next distinct minute', () => {
    const first = evaluateTick(new Date('2023-06-01T00:00:00Z'), everyMinute, 'UTC', INITIAL_TICK_STATE)
    const second = evaluateTick(new Date('2023-06-01T00:01:00Z'), everyMinute, 'UTC', first.nextState)
    expect(second.shouldFire).toBe(true)
  })

  it('suppresses a repeated tick within the same slot (timer jitter)', () => {
    const first = evaluateTick(new Date('2023-06-01T00:00:00Z'), everyMinute, 'UTC', INITIAL_TICK_STATE)
    const jitter = evaluateTick(new Date('2023-06-01T00:00:00.400Z'), everyMinute, 'UTC', first.nextState)
    expect(jitter.shouldFire).toBe(false)
    expect(jitter.nextState).toBe(first.nextState)
  })

  describe('clock stepped backward', () => {
    it('does not fire at an instant earlier than the previous fire', () => {
      const state = stateAfter('2023-6-1-0-5', '2023-06-01T00:05:00Z')
      const rewound = evaluateTick(new Date('2023-06-01T00:02:00Z'), everyMinute, 'UTC', state)
      expect(rewound.shouldFire).toBe(false)
    })

    it('resumes once real time passes the previous fire again', () => {
      const state = stateAfter('2023-6-1-0-5', '2023-06-01T00:05:00Z')
      const caughtUp = evaluateTick(new Date('2023-06-01T00:06:00Z'), everyMinute, 'UTC', state)
      expect(caughtUp.shouldFire).toBe(true)
    })
  })

  describe('clock jumped forward', () => {
    it('fires for the current slot only and never backfills the skipped slots', () => {
      const state = stateAfter('2023-6-1-1-0', '2023-06-01T01:00:00Z')
      const afterJump = evaluateTick(new Date('2023-06-01T04:00:30Z'), topOfHour, 'UTC', state)
      expect(afterJump.shouldFire).toBe(true)
      expect(afterJump.nextState.lastFiredSlot).toBe('2023-6-1-4-0')
    })
  })

  describe('daylight-saving fall-back overlap', () => {
    const daily0130 = parseCron('30 1 * * *')

    it('fires at the first occurrence of the repeated hour', () => {
      const result = evaluateTick(new Date('2023-11-05T05:30:00Z'), daily0130, 'America/New_York', INITIAL_TICK_STATE)
      expect(result.shouldFire).toBe(true)
    })

    it('suppresses the second occurrence even when a later slot fired in between', () => {
      const state = stateAfter('2023-11-5-1-45', '2023-11-05T05:45:00Z')
      const secondPass = evaluateTick(new Date('2023-11-05T06:30:00Z'), daily0130, 'America/New_York', state)
      expect(secondPass.shouldFire).toBe(false)
    })

    it('suppresses the second occurrence of a 30-minute fall-back (Lord Howe Island)', () => {
      const daily0145 = parseCron('45 1 * * *')
      const state = stateAfter('2023-4-2-1-50', '2023-04-01T14:50:00Z')
      const secondPass = evaluateTick(new Date('2023-04-01T15:15:00Z'), daily0145, 'Australia/Lord_Howe', state)
      expect(secondPass.shouldFire).toBe(false)
    })
  })
})
