import { SirannonClient as SirannonClientValue } from '../../../../packages/ts/dist/client/index.mjs'

type SirannonClientCtor = typeof import('../../../../packages/ts/dist/client/index').SirannonClient
export type RemoteDatabase = import('../../../../packages/ts/dist/client/index').RemoteDatabase

export const SirannonClient = SirannonClientValue as SirannonClientCtor
export type SirannonClient = InstanceType<SirannonClientCtor>
