# syntax=docker/dockerfile:1
# Node load-generator image for the benchmark. One generator drives both engines through their
# native clients: Sirannon over its SDK's WebSocket transport and PostgreSQL over node-postgres.
#
# Trixie (glibc 2.41) matches the server image so the shared build layers align. python3, make,
# and g++ are present so better-sqlite3 compiles during the repo install that produces the SDK
# build the generator imports.
FROM node:24-trixie-slim AS build
WORKDIR /repo
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 make g++ \
    && rm -rf /var/lib/apt/lists/*
RUN corepack enable
COPY . .
# Scope the install to the SDK package and disable pnpm's multi-day release-age gate; a full
# workspace install pulls the example apps and stalls this benchmark image build on slow networks.
RUN printf 'packages:\n  - packages/ts\nminimumReleaseAge: 0\nstrictDepBuilds: false\nverifyDepsBeforeRun: false\nfetchRetries: 5\nfetchTimeout: 600000\nfetchRetryMaxtimeout: 300000\nallowBuilds:\n  better-sqlite3: true\n  esbuild: true\n' > pnpm-workspace.yaml
RUN --mount=type=cache,id=pnpm-store,target=/pnpm/store \
    pnpm install --no-frozen-lockfile --store-dir=/pnpm/store
RUN pnpm --filter @delali/sirannon-db build
WORKDIR /repo/benchmarks/server/driver
RUN --mount=type=cache,id=pnpm-store,target=/pnpm/store \
    pnpm install --ignore-workspace --frozen-lockfile --store-dir=/pnpm/store

FROM node:24-trixie-slim AS runtime
WORKDIR /repo/benchmarks/server/driver
COPY --from=build /repo /repo
ENTRYPOINT ["node", "src/cli.ts"]
