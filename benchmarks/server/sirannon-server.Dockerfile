# Sirannon HTTP server image for the benchmark.
#
# Trixie (glibc 2.41) is chosen because the uWebSockets.js arm64 prebuilt the server loads
# requires a newer glibc than bookworm ships, so the image starts on both x86_64 and arm64.
# python3, make, and g++ are present so better-sqlite3 compiles its native addon during install.
FROM node:24-trixie-slim AS build
WORKDIR /repo
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 make g++ \
    && rm -rf /var/lib/apt/lists/*
RUN corepack enable
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @delali/sirannon-db build

FROM node:24-trixie-slim AS runtime
WORKDIR /repo
RUN corepack enable
COPY --from=build /repo /repo
ENV HOST=0.0.0.0 PORT=9876
EXPOSE 9876
CMD ["node", "benchmarks/server/sirannon-server.mjs"]
