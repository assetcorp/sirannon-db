# Security policy

## Supported versions

The maintainer publishes security fixes in the latest release on npm. Upgrade to the latest version before you report a problem, in case a fix already exists.

## Reporting a vulnerability

Report security issues privately through GitHub. Open the repository's **Security** tab and choose **Report a vulnerability**, or go straight to [the advisory form](https://github.com/assetcorp/sirannon-db/security/advisories/new). GitHub keeps the report private until a fix is ready.

Keep a security problem out of public issues and out of public discussion until the maintainer releases a fix.

Include as much of the following as you can:

- Give the affected version and the runtime, whether that's Node.js, Bun, a browser, or React Native.
- Name the part with the problem: the core engine, the server, the client, replication, or a driver.
- List the steps to reproduce the problem, and attach a proof of concept if you have one.
- Describe the impact that you expect, such as data loss, denial of service, or information disclosure.

## A note on the server

The built-in server refuses SQL from the network until you set `acceptSql: true`. Once you set it, the server executes the statements that callers send, apart from the few that it refuses with `FORBIDDEN_SQL`, such as a statement against Sirannon's internal `_sirannon` tables.

The server identifies each caller through the `authenticate` hook, which it calls before every database route and every WebSocket upgrade. A server without that hook answers every caller that can reach it, so the maintainer treats an unauthenticated server on a public network as an operator mistake, outside the scope of this policy.

The server applies no rate limit and no query timeout. It listens on plain HTTP and WebSocket, so terminate TLS at a proxy in front of it. Read the [security section of the package README](../packages/ts/README.md#security) for the deployment boundary, authentication, and TLS.

## What to expect

The maintainer acknowledges every report. Once the maintainer confirms the problem, they'll prepare a fix and coordinate its disclosure with you. Coordinator-backed automatic failover and the etcd coordinator are experimental, so their design may still change.
