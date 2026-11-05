# A rebuilt copy keeps its node ID, and the group trusts it only once it proves that it is current

When an operator moves a copy of a replicated database to another server or replaces its disk, the copy restarts under its own node ID with an empty or replaced file. In coordinator mode, that copy used to keep its place in the in-sync set across the restart, so the controller could promote an empty file that then refused every read and write. The primary also counted the acknowledgements that the copy had sent before the restart, which let it readmit a copy whose full copy had not finished. This decision replaces the rule in `packages/spec/09-failover.md` that gives a new empty node a new identity.

## Why this over the alternatives

- **A new node ID for every new file.** etcd replaces a failed machine by removing the member and adding a new one, and the new member receives a new ID. In Sirannon, however, a new ID turns every disk replacement into a change of voters, although the group gains no new member. MongoDB keeps the member instead, since its resync procedure restarts `mongod` with an empty data directory and lets initial sync restore the data.
- **Trusting a copy that restarts with the file it had.** A session in the coordinator records the node ID and nothing about the file behind it, so the group learns nothing from a restart about what the file holds.
- **Keeping acknowledgements across connections.** An acknowledgement describes the file that sent it, and an operator can swap that file between one connection and the next.

## Decision

- A copy that restarts with an empty or replaced file keeps its node ID and rebuilds in place. It copies the whole database from the primary, and it rejoins the in-sync set only once it proves that it holds every acknowledged write.
- The group treats every restart as unproven. Before a restarted copy registers its session, it leaves the in-sync set through one write to the coordinator, and it refuses majority reads until the primary readmits it.
- The primary forgets a copy's acknowledged position whenever that copy opens a new connection, so only the acknowledgements that arrive over the current connection can readmit the copy.

## Cost

Each restart adds one write to the coordinator, and a restarted copy serves no majority reads until the primary readmits it. While one copy of a three-voter group rebuilds, two current copies remain, so a second failure during the rebuild stops majority writes until the rebuild finishes.

## References

- etcd runtime reconfiguration: <https://etcd.io/docs/v3.5/op-guide/runtime-configuration/>
- MongoDB resync of a replica set member: <https://www.mongodb.com/docs/manual/tutorial/resync-replica-set-member/>
- MongoDB replica set member states: <https://www.mongodb.com/docs/manual/reference/replica-states/>
