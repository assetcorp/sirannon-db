// The load generator drives Sirannon through the exact SDK an application ships with. The client
// is consumed from the package's built output by relative path, the same way the benchmark server
// entry point loads the core and server builds, so no publish or workspace link is required.
//
// tsup emits `index.d.ts` beside `index.mjs`, which bundler module resolution does not associate
// with a `.mjs` import on its own. The runtime value therefore comes from the `.mjs`, while the
// types come from the sibling declaration through an `import(...)` type query, so the generator is
// checked against the real client surface it drives.

import { SirannonClient as SirannonClientValue } from '../../../../packages/ts/dist/client/index.mjs'

type SirannonClientCtor = typeof import('../../../../packages/ts/dist/client/index').SirannonClient
export type RemoteDatabase = import('../../../../packages/ts/dist/client/index').RemoteDatabase

export const SirannonClient = SirannonClientValue as SirannonClientCtor
export type SirannonClient = InstanceType<SirannonClientCtor>
