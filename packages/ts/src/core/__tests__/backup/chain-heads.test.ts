import { describe, expect, it } from 'vitest'
import { appendChainHead, type BackupChainHead, readChainHeads } from '../../backup/chain.js'
import type { BackupDestination } from '../../backup/destination.js'
import type { SirannonError } from '../../errors.js'
import { type MemoryDestination, memoryDestination } from './memory-destination.js'

const CHAIN_NAME = 'sirannon-backup-chain'

function headFor(chainId: string): BackupChainHead {
  return { chainId, startedAt: 1_700_000_000_000 }
}

function contested(destination: MemoryDestination, competitors: number): BackupDestination {
  const encoder = new TextEncoder()
  let displaced = 0
  return {
    ...destination,
    async writePiece(name, index, bytes) {
      await destination.writePiece(name, index, bytes)
      if (name !== CHAIN_NAME || displaced >= competitors) return
      displaced++
      await destination.writePiece(name, index, encoder.encode(JSON.stringify(headFor(`other-${displaced}`))))
    },
  }
}

describe('adding a chain to the list a destination holds', () => {
  it('puts the first chain at index zero', async () => {
    const destination = memoryDestination()

    expect(await appendChainHead(destination, CHAIN_NAME, headFor('first'))).toBe(0)
  })

  it('puts each later chain after the ones already there', async () => {
    const destination = memoryDestination()
    await appendChainHead(destination, CHAIN_NAME, headFor('first'))

    expect(await appendChainHead(destination, CHAIN_NAME, headFor('second'))).toBe(1)
  })

  it('moves on where another node took the same place, and loses neither chain', async () => {
    const destination = memoryDestination()
    const index = await appendChainHead(contested(destination, 1), CHAIN_NAME, headFor('ours'))
    const heads = await readChainHeads(destination, CHAIN_NAME)

    expect(index).toBe(1)
    expect(heads.map(head => head.chainId).sort()).toEqual(['other-1', 'ours'])
  })

  it('leaves the places of a chain an operator deleted alone', async () => {
    const destination = memoryDestination()
    await destination.writePiece(CHAIN_NAME, 5, new TextEncoder().encode(JSON.stringify(headFor('kept'))))

    expect(await appendChainHead(destination, CHAIN_NAME, headFor('next'))).toBe(6)
  })

  it('gives up where another chain takes every place it tries, and names the chain', async () => {
    const appending = appendChainHead(contested(memoryDestination(), 20), CHAIN_NAME, headFor('crowded'))

    const err = (await appending.catch((thrown: unknown) => thrown)) as SirannonError
    expect(err.code).toBe('BACKUP_DESTINATION_ERROR')
    expect(err.message).toContain("chain 'crowded'")
  })
})
