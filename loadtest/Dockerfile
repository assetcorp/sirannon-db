FROM node:24-trixie-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates git python3 make g++ \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN corepack enable

COPY pnpm-workspace.yaml pnpm-lock.yaml package.json tsconfig.json ./
COPY packages/ts/package.json packages/ts/
RUN pnpm install --filter @delali/sirannon-db --ignore-scripts=false --config.confirmModulesPurge=false || pnpm install --filter @delali/sirannon-db

COPY packages/ts packages/ts
RUN pnpm --filter @delali/sirannon-db exec tsup

COPY benchmarks/server/sirannon-server.mjs benchmarks/server/

ENV HOST=0.0.0.0
ENV PORT=9876
ENV BENCH_SIRANNON_DB=bench
ENV BENCH_DATA_DIR=/data

CMD ["node", "benchmarks/server/sirannon-server.mjs"]
