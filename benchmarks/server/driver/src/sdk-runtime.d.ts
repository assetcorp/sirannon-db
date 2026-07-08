// The SDK's built `.mjs` has no `.d.mts` beside it, so bundler resolution cannot type the runtime
// import directly. This declares that module as untyped; the real types are recovered separately
// through an `import(...)` type query against the sibling `index.d.ts` in sirannon-client.ts.
declare module '*/dist/client/index.mjs'
