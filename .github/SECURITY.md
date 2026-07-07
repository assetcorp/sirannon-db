# Security policy

## Supported versions

Security fixes go into the latest published release on npm. Upgrade to the latest version before you report a problem, in case a fix already exists.

## Reporting a vulnerability

Report security issues privately through GitHub. Open the repository's **Security** tab and choose **Report a vulnerability**, or go straight to [the advisory form](https://github.com/assetcorp/sirannon-db/security/advisories/new). This keeps the report private until a fix is ready.

Please do not open a public issue for a security problem, and please do not disclose it publicly before a fix is deployed.

Include as much as you can:

- The affected version and the runtime, whether Node.js, Bun, a browser, or React Native.
- Which part is affected: the core engine, the server, the client, replication, or a driver.
- The steps to reproduce, and a proof of concept if you have one.
- The impact you expect, such as data loss, denial of service, or information disclosure.

## A note on the server

The built-in server runs SQL sent by a client and is unauthenticated unless you supply an `onRequest` hook. It has no rate limiting, no query timeout, and no SQL allowlist, and it binds plain HTTP and WebSocket without TLS. This is by design: the server is a low-level database endpoint, not a public application API. Running an unauthenticated instance on a public network is an open database, so treat that as an operator mistake rather than a vulnerability in Sirannon. The [security section of the package documentation](../packages/ts/README.md#security) covers the deployment boundary, authentication, and TLS.

## What to expect

You will get an acknowledgement of your report. Once the issue is confirmed, a fix and a coordinated disclosure will follow. Coordinator-backed automatic failover and the etcd coordinator are experimental, so that work is still in progress.
