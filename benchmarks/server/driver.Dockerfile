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
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @delali/sirannon-db build
WORKDIR /repo/benchmarks/server/driver
RUN pnpm install --ignore-workspace --frozen-lockfile

FROM node:24-trixie-slim AS runtime
WORKDIR /repo/benchmarks/server/driver
COPY --from=build /repo /repo
ENTRYPOINT ["node", "src/cli.ts"]
