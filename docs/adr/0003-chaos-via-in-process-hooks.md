# Replication chaos tests use in-process hooks, not a TCP proxy

The chaos layer must reproduce four scenarios from the plan: connection drops mid-batch, replica restart mid-sync, primary crash and recovery, and slow acks on the CDC prune back-pressure path. We cover all four with `transport.disconnect()`/reconnect (exercises real gRPC reconnect), engine `stop()`/`start()` against persistent temp dirs (real on-disk state survives), and a debug-only `__setAckDelayMs(ms)` hook on the engine for ack throttling. No TCP proxy.

## Rejected alternatives

- **Toxiproxy** (Shopify) is the obvious choice for TCP-level chaos, but it requires an external binary running alongside tests. Adopting it via `toxiproxy-node-client` plus Testcontainers pulls Docker into the test setup, slows boot, and complicates CI for value the four scenarios do not need.
- **Trixter** (Rust, Oct 2025) has the same external-binary cost with a less mature client surface for Node.
- **toxy** (h2non) is HTTP-only and does not deal cleanly with gRPC bidirectional streams.
- **A custom in-process TCP proxy** would solve the problem with no external dep but adds ~200–300 lines of code we have no current use for; the four scenarios are covered without it.

## When to revisit

If a future scenario needs byte-level chaos that the in-process hooks cannot express — for example, half-broken connections, throughput throttling per byte, or simulated jitter — we adopt Toxiproxy at that point rather than pre-emptively. The `__setAckDelayMs` hook is explicitly debug-only and stays out of the public API.
