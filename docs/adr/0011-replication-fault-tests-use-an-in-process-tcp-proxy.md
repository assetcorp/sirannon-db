# Replication fault tests put an in-process TCP proxy between nodes

The replication end-to-end tests have to reproduce the faults that a node meets on a real network: a link that drops while batches are in flight, acknowledgements that arrive late, a connection that stays open while no bytes cross it, and a peer that refuses connections while it restarts. A replica in those tests can dial its primary through `src/__tests__/e2e/lib/fault-proxy.ts`, a TCP proxy that runs inside the test process. The test gives the replica the proxy's port as the primary's port, and the proxy can then delay each direction, stall and resume traffic, cut every open connection, refuse new connections, and forward new connections to another port. This decision supersedes [ADR 0003](0003-chaos-via-in-process-hooks.md).

## Why this over the alternatives

- **The engine hook from ADR 0003.** The `__setAckDelayMs` method was installed on every `ReplicationEngine` that Sirannon constructed, including the engines in production, and it could only delay acknowledgements. The proxy delays acknowledgements as well, so the hook and `replication/engine/test-hooks.ts` are gone.
- **Calling `transport.disconnect()` and `connect()` from the test.** A test that reconnects by hand cannot show that the transport redials by itself once a peer comes back, which is what a node depends on after a real outage. A cut connection through the proxy leaves the redial to the transport.
- **Toxiproxy for the end-to-end suite.** Toxiproxy needs Docker, and the end-to-end suite runs on every pull request without it. ADR 0003 rejected a custom proxy because the four scenarios of that time did not need one, and it estimated the proxy at 200 to 300 lines; the proxy that we wrote is 208 lines.

## Decision

- The end-to-end replication suite produces network faults through the in-process proxy, and the simulated transport produces the faults that need message-level control, such as losing every acknowledgement or a seeded three messages in ten.
- The coordinator failover suite keeps Toxiproxy, because it already starts etcd in Docker and runs each node in a child process of its own.
- Production code carries no hook that exists only for a test.
