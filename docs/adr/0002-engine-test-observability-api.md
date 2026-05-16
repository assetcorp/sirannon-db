# Engine exposes getAppliedSeq and getCurrentSeq for convergence checks

E2E and chaos tests need a deterministic way to assert "the replica has caught up to where the primary was at the moment of this write". `ReplicationEngine` gains two narrow public methods: `getAppliedSeq(peerId): bigint` and `getCurrentSeq(): bigint`. A shared `waitForReplica(replica, primaryId, targetSeq, timeoutMs)` helper polls until convergence and surfaces lag (`expected: X, got: Y, lag: Z`) in its timeout error.

## Why this over the alternatives

- **Polling SELECTs against the replica** works for happy-path positive checks but cannot express negative assertions ("verify row X is not yet visible") and gives no diagnostic when it times out.
- **Subscribing to a `batch-applied` event** is push-based but creates a timing trap: a test that subscribes after the write misses the event and hangs.

The seq-polling approach handles positive checks, negative checks, and pre-quiesce assertions with one mechanism.

## Cost

Two engine seq numbers are now part of the public contract. They already exist internally (`log.getLastAppliedSeq`, `highestSourceSeqSeen`); we are formalising the read surface, not adding new state.
