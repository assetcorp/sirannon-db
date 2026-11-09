"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator. Do not edit."""

from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias

from pydantic import Field

from sirannon_client._model import OPEN_ENUM, ApiModel


class BackupChainBase(ApiModel):
    """The full copy at the head of a chain. Sirannon restores from this copy and then applies the
    change pieces on top of it.
    """

    kind: Literal["full"]
    """The kind of record."""
    chain_id: str = Field(alias="chainId")
    """The chain that this copy begins."""
    name: str
    """The name that Sirannon stores the pieces of this copy under at the destination."""
    run_id: str = Field(alias="runId")
    """The identifier that Sirannon reports the progress of this copy under."""
    finished_at: int | float = Field(alias="finishedAt")
    """The moment, in epoch milliseconds, that the copy finished. A restore to any earlier moment
    needs an older chain.
    """
    piece_count: int | float = Field(alias="pieceCount")
    """The number of pieces that Sirannon stores the copy in."""
    piece_bytes: int | float = Field(alias="pieceBytes")
    """The size of one whole piece, in bytes."""
    bytes_written: int | float = Field(alias="bytesWritten")
    """The number of bytes that Sirannon stores at the destination."""
    fingerprint: str | None = None
    """The SHA-256 of the copy, present while fingerprinting is on."""
    key_id: str | None = Field(default=None, alias="keyId")
    """The identifier that came with the key that this copy holds ciphertext under, where the key
    came with one.
    """


class BackupChainPosition(ApiModel):
    """The range of write-ahead log frames that one change piece holds."""

    log_sequence: int | float = Field(alias="logSequence")
    """The checkpoint sequence of the log that holds these frames. SQLite adds one to it each time
    it restarts the log.
    """
    salt1: int | float
    """The first salt of that log. Together with `salt2`, it identifies the generation of the log
    that holds the frames.
    """
    salt2: int | float
    """The second salt of that log."""
    first_frame: int | float = Field(alias="firstFrame")
    """The first frame in the piece, counted from one."""
    last_frame: int | float = Field(alias="lastFrame")
    """The last frame in the piece."""


class BackupChainChange(ApiModel):
    """One change piece, which holds the log frames that a database writes between the previous
    capture and this one.
    """

    kind: Literal["change"]
    """The kind of record."""
    chain_id: str = Field(alias="chainId")
    """The chain that this piece extends, which identifies the full copy that the piece applies on
    top of.
    """
    name: str
    """The name that Sirannon stores this piece under at the destination."""
    run_id: str = Field(alias="runId")
    """The identifier that Sirannon reports the progress of this capture under."""
    sequence: int | float
    """The position of this piece in its chain, counted from one."""
    position: BackupChainPosition
    """The range of log frames that this piece holds."""
    captured_at: int | float = Field(alias="capturedAt")
    """The moment, in epoch milliseconds, that Sirannon captured the frames. A restore stops at a
    capture time such as this one.
    """
    frame_count: int | float = Field(alias="frameCount")
    """The number of log frames in this piece."""
    piece_count: int | float = Field(alias="pieceCount")
    """The number of destination pieces that Sirannon stores these frames in."""
    piece_bytes: int | float = Field(alias="pieceBytes")
    """The size of one whole piece, in bytes."""
    bytes_written: int | float = Field(alias="bytesWritten")
    """The number of bytes that Sirannon stores at the destination."""
    checkpointed: bool
    """Whether the checkpoint after this capture emptied the log."""
    fingerprint: str | None = None
    """The SHA-256 of the frames, present while fingerprinting is on."""
    key_id: str | None = Field(default=None, alias="keyId")
    """The identifier that came with the key that these frames hold ciphertext under, where the key
    came with one.
    """


class BackupChain(ApiModel):
    """One full copy and every change piece that Sirannon captures after it."""

    chain_id: str = Field(alias="chainId")
    """The identifier of this chain."""
    started_at: int | float = Field(alias="startedAt")
    """The moment, in epoch milliseconds, that Sirannon started the chain."""
    previous_chain_id: str | None = Field(default=None, alias="previousChainId")
    """The identifier of the previous chain, where this chain replaces one."""
    base: BackupChainBase | None = None
    """The full copy that the chain starts from. After someone deletes that record, this field is
    absent, so no restore can use the chain.
    """
    changes: list[BackupChainChange]
    """The change pieces, oldest first."""


BackupChainRecord: TypeAlias = BackupChainBase | BackupChainChange
"""Either the full copy at the head of a chain or one change piece along it."""


class BackupChainResponse(ApiModel):
    """Every chain in the backup destination."""

    chains: list[BackupChain]
    """One entry per chain, newest first, each with its full copy and its change pieces in order."""


class BackupProgress(ApiModel):
    """The progress of one backup, which Sirannon reports after each step of the copy and after
    each piece that it stores.
    """

    run_id: str = Field(alias="runId")
    """The identifier of the backup that this progress describes."""
    phase: Annotated[Literal["copy", "transfer"], OPEN_ENUM]
    """Whether Sirannon is copying pages or sending pieces."""
    total_pages: int | float = Field(alias="totalPages")
    """The total number of pages to copy. For a change piece, this is the number of log frames in
    the piece.
    """
    remaining_pages: int | float = Field(alias="remainingPages")
    """The number of pages left to copy. On the staged route, every report of the transfer phase
    states zero, since the copy finishes before the transfer starts. On the streamed route, the
    two phases overlap, so a transfer report states the pages left at the latest step.
    """
    restarts: int | float
    """The number of times so far that SQLite restarts the copy from page one. A change piece
    reports zero, since Sirannon reads the log for it and copies no pages.
    """
    pieces_written: int | float = Field(alias="piecesWritten")
    """The number of pieces stored at the destination so far."""
    bytes_written: int | float = Field(alias="bytesWritten")
    """The number of bytes stored at the destination so far."""


class BackupCycleError(ApiModel):
    """A failure of one turn of the cycle."""

    code: str
    """The error code, which a caller matches on."""
    message: str
    """A sentence for the log of the operator that describes the failure."""
    at: int | float
    """The moment, in epoch milliseconds, that Sirannon recorded the failure."""
    duration_ms: int | float = Field(alias="durationMs")
    """The time from the start of the turn to the failure, in milliseconds."""
    chain_id: str | None = Field(default=None, alias="chainId")
    """The chain that the cycle held when the failure occurred, where it held one."""
    progress: BackupProgress | None = None
    """The progress of the backup at the failure. Sirannon leaves this field out for a failure
    before the copy begins. For a failure part-way through, it records the backup identifier,
    the pages left to copy, and the pieces and bytes already stored at the destination.
    """


class BackupLogPosition(ApiModel):
    """The position of the write-ahead log of a database at one moment. A full copy records this
    position, so that you can tell which generation of the log the database was on when the copy
    finished. SQLite restarts the log after a checkpoint that empties it, and it writes a new
    pair of salts into the log at each restart. The backup cycle runs a checkpoint after each
    capture and none between a full copy and the first change piece of its chain, so those two
    records hold the same salts. A change piece holds later salts than the piece before it
    whenever the checkpoint between the two empties the log. A reader can keep a checkpoint from
    emptying the log, which leaves the log on its current generation, so two consecutive change
    pieces can hold the same salts.
    """

    log_sequence: int | float = Field(alias="logSequence")
    """The checkpoint sequence of the log. SQLite adds one to it each time it restarts the log."""
    salt1: int | float
    """The first salt of that log. Together with `salt2`, it identifies the generation of the log.
    """
    salt2: int | float
    """The second salt of that log."""
    last_frame: int | float = Field(alias="lastFrame")
    """The last frame that commits a transaction, counted from one, or zero where the log holds no
    such frame.
    """


class BackupRestoreProgress(ApiModel):
    """The progress of one restore, which Sirannon reports after every piece that it fetches and
    after every batch.
    """

    phase: Annotated[Literal["full-copy", "changes"], OPEN_ENUM]
    """Whether Sirannon is fetching the full copy or applying the change pieces on top of it."""
    pieces_fetched: int | float = Field(alias="piecesFetched")
    """The number of pieces fetched so far."""
    bytes_fetched: int | float = Field(alias="bytesFetched")
    """The number of bytes fetched so far."""
    changes_applied: int | float = Field(alias="changesApplied")
    """The number of change pieces applied so far."""
    changes_total: int | float = Field(alias="changesTotal")
    """The total number of change pieces in the plan."""


class BackupRestoreRequest(ApiModel):
    """The moment to which the server rebuilds the database, and how many change pieces it replays
    in one batch.
    """

    moment: int | float | None = None
    """The moment, in epoch milliseconds, to restore the database to. When it is absent, the server
    uses the current time, so it restores the newest state that the destination stores.
    """
    batch_size: int | float | None = Field(default=None, alias="batchSize")
    """How many change pieces the server replays between one checkpoint and the next, from 1 to
    4096. Defaults to 16.
    """


BackupRestoreState: TypeAlias = Annotated[Literal["running", "idle", "done", "failed"], OPEN_ENUM]
"""The state of one database's restore. `idle` means that no restore has started since the server
started.
"""


class BackupRestoreStatusReopenError(ApiModel):
    """The error that stopped the rebuilt database from opening again. The rebuilt data is on disk,
    but the registry has no database open under the identifier, so restart the process when you
    see this error.
    """

    code: str
    """The machine-readable code of the error."""
    message: str
    """What went wrong, for a log or a person."""


class BackupRestoreStatusError(ApiModel):
    """The error that stopped a failed restore."""

    code: str
    """The machine-readable code of the error."""
    message: str
    """What went wrong, for a log or a person."""


class BackupRestoreSummary(ApiModel):
    """The report of one finished restore as the server sends it, which leaves out the path of the
    rebuilt database file on the server.
    """

    chain_id: str = Field(alias="chainId")
    """The chain that Sirannon restores from."""
    started_at: int | float = Field(alias="startedAt")
    """The moment, in epoch milliseconds, that the restore started."""
    finished_at: int | float = Field(alias="finishedAt")
    """The moment, in epoch milliseconds, that the restore finished."""
    duration_ms: int | float = Field(alias="durationMs")
    """The length of the restore, in milliseconds."""
    piece_count: int | float = Field(alias="pieceCount")
    """The number of stored pieces that Sirannon fetches, for the full copy and every change piece.
    """
    base_name: str = Field(alias="baseName")
    """The name that Sirannon stores the full copy of the chain under."""
    restores_to: int | float = Field(alias="restoresTo")
    """The moment, in epoch milliseconds, that the rebuilt database reflects, which is the capture
    time of the last change piece applied, or the finish time of the full copy where Sirannon
    applies none.
    """
    bytes_fetched: int | float = Field(alias="bytesFetched")
    """The number of bytes that Sirannon fetches."""
    changes_applied: int | float = Field(alias="changesApplied")
    """The number of change pieces that Sirannon applies."""
    frames_applied: int | float = Field(alias="framesApplied")
    """The number of log frames in those pieces."""
    batch_count: int | float = Field(alias="batchCount")
    """The number of batches that Sirannon applies the change pieces in, each with a checkpoint of
    its own.
    """


class BackupRestoreStatus(ApiModel):
    """What the server reports about the restore of one database."""

    state: BackupRestoreState
    """The state of the restore."""
    moment: int | float | None = None
    """The moment, in epoch milliseconds, that the caller asked to restore to."""
    started_at: int | float | None = Field(default=None, alias="startedAt")
    """When the restore started, in epoch milliseconds."""
    finished_at: int | float | None = Field(default=None, alias="finishedAt")
    """When the restore finished, in epoch milliseconds, whether it succeeded or failed."""
    progress: BackupRestoreProgress | None = None
    """The restore's progress, which the server updates after it fetches each piece."""
    report: BackupRestoreSummary | None = None
    """The report of a finished restore, without the path of the rebuilt database file."""
    reopen_error: BackupRestoreStatusReopenError | None = Field(default=None, alias="reopenError")
    """The error that stopped the rebuilt database from opening again. The rebuilt data is on disk,
    but the registry has no database open under the identifier, so restart the process when you
    see this error.
    """
    error: BackupRestoreStatusError | None = None
    """The error that stopped a failed restore."""


class BackupRunSummary(ApiModel):
    """The report of one finished backup as the server sends it, which leaves out the path of the
    source database file on the server.
    """

    database_id: str = Field(alias="databaseId")
    """The identifier of the source database."""
    chain_id: str = Field(alias="chainId")
    """The chain that this backup is part of. A full copy begins a chain, and every later change
    piece extends it.
    """
    run_id: str = Field(alias="runId")
    """The identifier that Sirannon reports the progress of this backup under."""
    kind: Annotated[Literal["full", "change"], OPEN_ENUM]
    """Whether the backup holds the whole database or the log frames written since the previous
    capture.
    """
    route: Annotated[Literal["staged", "streamed"], OPEN_ENUM]
    """Whether Sirannon sends the bytes through a local file, which is `staged`, or without one,
    which is `streamed`.
    """
    destination_name: str = Field(alias="destinationName")
    """The name that Sirannon stores the pieces under at the destination. Sirannon writes only this
    name at the destination, because the journal that SQLite opens next to a copy stays on local
    disk.
    """
    started_at: int | float = Field(alias="startedAt")
    """The moment, in epoch milliseconds, that the backup started."""
    finished_at: int | float = Field(alias="finishedAt")
    """The moment, in epoch milliseconds, that the backup finished."""
    duration_ms: int | float = Field(alias="durationMs")
    """The length of the whole backup, in milliseconds."""
    copy_ms: int | float = Field(alias="copyMs")
    """The time, in milliseconds, that SQLite spends copying pages."""
    transfer_ms: int | float = Field(alias="transferMs")
    """The time, in milliseconds, that Sirannon spends storing pieces at the destination."""
    page_count: int | float = Field(alias="pageCount")
    """The number of pages that SQLite copies."""
    page_size: int | float = Field(alias="pageSize")
    """The size of one page, in bytes."""
    bytes_written: int | float = Field(alias="bytesWritten")
    """The number of bytes that Sirannon stores at the destination."""
    piece_count: int | float = Field(alias="pieceCount")
    """The number of pieces that Sirannon stores at the destination."""
    piece_bytes: int | float = Field(alias="pieceBytes")
    """The size of one whole piece, in bytes."""
    restarts: int | float
    """The number of times that SQLite restarts the copy from page one."""
    position: BackupChainPosition | None = None
    """The range of log frames that a change piece holds. A full copy leaves this field out."""
    log_position: BackupLogPosition | None = Field(default=None, alias="logPosition")
    """The position of the write-ahead log when a full copy finishes. Sirannon reads the log after
    SQLite copies the last page, so if a writer commits between those two steps, this position
    can include a frame that the copy lacks. A change piece states its range of log frames in
    BackupRunReport.position, and a database without a write-ahead log leaves out both fields.
    """
    fingerprint: str | None = None
    """The SHA-256 of the copied file, present while fingerprinting is on."""


class BackupSafeToDeleteRequest(ApiModel):
    """How far back you must still be able to restore."""

    restorable_from: int | float | None = Field(default=None, alias="restorableFrom")
    """The earliest moment, in epoch milliseconds, that you must still be able to restore to. When
    it is absent, the server lists only the backups that you cannot restore from.
    """


class BackupSafeToDeleteResponse(ApiModel):
    """The records that you no longer need for any restore."""

    records: list[BackupChainRecord]
    """The records that you can delete, oldest first."""


BackupSkipReason: TypeAlias = Annotated[Literal["not-preferred", "group-unavailable", "previous-run-active"], OPEN_ENUM]
"""The reason that one turn of the cycle writes nothing. `'not-preferred'` means that another node, or
no node, takes the backups of this group. `'group-unavailable'` means that Sirannon cannot read the
membership of the group. `'previous-run-active'` means that the previous turn is still in progress.
"""


class BackupSkip(ApiModel):
    """One turn that the cycle skips, and the reason for the skip."""

    reason: BackupSkipReason
    """The condition that applies."""
    message: str
    """A sentence for the log of the operator that describes the skip."""
    node_id: str | None = Field(default=None, alias="nodeId")
    """The identifier of this node, where a group source supplies one."""
    preferred_node_id: str | None = Field(default=None, alias="preferredNodeId")
    """The identifier of the node that takes the backups, where Sirannon picks one."""
    uncaptured_log_bytes: int | float | None = Field(default=None, alias="uncapturedLogBytes")
    """The size in bytes of the write-ahead log on this node at the skip. An alert on this value as
    it rises warns you that the log on a node is growing, well before `maxUncapturedLogBytes`
    ends its chain.
    """


class BackupStatusResponse(ApiModel):
    """The checkpoint cycle's current activity, and the results of its recent turns, as the server
    sends them.
    """

    last_run: BackupRunSummary | None = Field(default=None, alias="lastRun")
    """The report of the most recent backup that the cycle finished, without the path of the source
    database file.
    """
    running: bool
    """Whether a turn is in progress."""
    chain_id: str | None = Field(default=None, alias="chainId")
    """The chain that the cycle extends, where it holds one."""
    progress: BackupProgress | None = None
    """The progress of the current turn, which Sirannon clears between turns."""
    last_skip: BackupSkip | None = Field(default=None, alias="lastSkip")
    """The most recent skipped turn, and the reason for the skip."""
    last_error: BackupCycleError | None = Field(default=None, alias="lastError")
    """The most recent failure of a turn."""


class BackupTriggerResponse(ApiModel):
    """The server's answer when a caller triggers a backup, which it sends once it accepts the turn
    and before the backup finishes.
    """

    started: Literal[True]
    """Always true, because the server sends this answer only after it accepts the turn."""


class BackupVerifyRequest(ApiModel):
    """The backup to read back from the destination."""

    name: str
    """The name under which the destination stores the backup, as it appears in every chain record.
    """


class BackupVerifyResponse(ApiModel):
    """The result of reading one stored backup from a destination and checking it against its
    record.
    """

    name: str
    """The name that Sirannon stores the pieces under."""
    chain_id: str = Field(alias="chainId")
    """The chain that holds this record."""
    kind: Annotated[Literal["full", "change"], OPEN_ENUM]
    """Whether this is the full copy at the head of that chain or one change piece along it."""
    piece_count: int | float = Field(alias="pieceCount")
    """The number of pieces at the destination."""
    bytes_read: int | float = Field(alias="bytesRead")
    """The total size of those pieces, in bytes."""
    fingerprint: str | None = None
    """The SHA-256 that Sirannon computes over the bytes that it reads, present where the record
    holds a fingerprint to compare against.
    """


WriteConcernLevel: TypeAlias = Annotated[Literal["local", "majority", "all"], OPEN_ENUM]
"""Which nodes must acknowledge a write before the call returns."""


class WriteConcern(ApiModel):
    """Which nodes must acknowledge a write, and how long the caller waits for their
    acknowledgements.
    """

    level: WriteConcernLevel
    """Which nodes must acknowledge the write before the call returns."""
    timeout_ms: int | float | None = Field(default=None, alias="timeoutMs")
    """The number of milliseconds to wait for those acknowledgements before the call fails."""


class BatchRequest(ApiModel):
    """The body of `POST /db/{id}/batch`, which the server commits atomically in one transaction
    with one fsync.
    """

    sql: str
    """The statement to execute for each parameter set."""
    params_batch: list[dict[str, Any] | list[Any]] = Field(alias="paramsBatch")
    """One parameter set for each execution of the statement."""
    write_concern: WriteConcern | None = Field(default=None, alias="writeConcern")
    """The acknowledgements that the server waits for before it confirms the batch."""


class ExecuteResponse(ApiModel):
    """The body that a write route returns."""

    changes: int | float
    """The number of rows that the statement inserted, updated, or deleted."""
    last_insert_row_id: str | int | float = Field(alias="lastInsertRowId")
    """The row id that SQLite assigned to the last inserted row, as a decimal string when it
    exceeds the safe integer range.
    """


class BatchResponse(ApiModel):
    """The body that a batch route returns."""

    results: list[ExecuteResponse]
    """One result per parameter set, in order."""


class BlobEnvelope(ApiModel):
    """A BLOB, which JSON has no type for. A row, a change event, a bind parameter, and a
    subscription filter carry every BLOB in this envelope. Decode it into a byte array, and send
    a byte array in the same envelope.
    """

    sirannon_blob: str = Field(alias="__sirannon_blob")
    """The bytes in uppercase hexadecimal, two digits per byte, where an empty string is an empty
    BLOB.
    """


BulkLoadDurability: TypeAlias = Annotated[Literal["off", "normal"], OPEN_ENUM]
"""The synchronous level during a bulk load. At 'off', SQLite documents that a power loss or an
operating-system crash can corrupt the database, so 'off' suits only a load that the operator can
re-run from scratch. At 'normal', a WAL-mode database stays safe from corruption, which suits a load
into a database whose existing data the operator must keep.
"""


class CapabilitiesResponseRegistry(ApiModel):
    """The digest of the registered operations, which the server sends when it serves any."""

    digest: str
    """A hash over every registered database identifier, operation kind, operation name, and
    argument name.
    """


class CapabilitiesResponse(ApiModel):
    """The body of `GET /capabilities`, which names what this server accepts."""

    capabilities: list[str]
    """The capabilities that the server announces, such as `query.sql` when it accepts SQL
    statements and `query.named` when it serves registered operations.
    """
    registry: CapabilitiesResponseRegistry | None = None
    """The digest of the registered operations, which the server sends when it serves any."""


class SyncBatchHlcRange(ApiModel):
    """The lowest and the highest hybrid logical clock stamp in the batch."""

    min: str
    """The lowest stamp in the batch."""
    max: str
    """The highest stamp in the batch."""


class SyncChange(ApiModel):
    """One row change inside a device-sync batch, with each column value in its tagged wire form."""

    table: str
    """The table of the changed row."""
    operation: str
    """Whether the device inserted, updated, or deleted the row."""
    row_id: str = Field(alias="rowId")
    """The primary key of the changed row, encoded as a string."""
    primary_key: dict[str, Any] = Field(alias="primaryKey")
    """The primary key columns of the changed row and their values."""
    hlc: str
    """The hybrid logical clock stamp that the device gave this change."""
    tx_id: str = Field(alias="txId")
    """The identifier of the device transaction that made this change."""
    node_id: str = Field(alias="nodeId")
    """The 32-character hexadecimal identifier of the device, which equals the batch's
    `sourceNodeId`.
    """
    new_data: dict[str, Any] | None = Field(alias="newData")
    """The row's values after the change, or null for a delete."""
    old_data: dict[str, Any] | None = Field(alias="oldData")
    """The row's values before the change, or null for an insert."""
    seq: str | None = None
    """The position of this change in the device's change log, as a decimal string."""


class SyncBatch(ApiModel):
    """The batch of row changes that a device pushes, with the range of the device's change log
    that it covers.
    """

    source_node_id: str = Field(alias="sourceNodeId")
    """The 32-character hexadecimal identifier of the device that pushes the batch."""
    batch_id: str = Field(alias="batchId")
    """The device's identifier for this batch, of 1 to 128 characters."""
    from_seq: str = Field(alias="fromSeq")
    """The first change sequence that the batch covers, as a decimal string."""
    to_seq: str = Field(alias="toSeq")
    """The last change sequence that the batch covers, as a decimal string."""
    hlc_range: SyncBatchHlcRange = Field(alias="hlcRange")
    """The lowest and the highest hybrid logical clock stamp in the batch."""
    changes: list[SyncChange]
    """The changes, from 1 to 1000 of them, in the order that the device made them."""
    checksum: str
    """The checksum over the batch's changes, which the server recomputes before it applies them."""
    tx_continues: bool | None = Field(default=None, alias="txContinues")
    """Set when the batch ends part-way through a device transaction that the next batch continues.
    """


class ChangesRequest(ApiModel):
    """The body of `POST /db/{id}/changes`, which applies a device's changes to the server's
    database.
    """

    schema_version: int | float | None = Field(default=None, alias="schemaVersion")
    """The schema version of the device's database, which the server compares with its own before
    it applies the batch.
    """
    batch: SyncBatch
    """The changes to apply."""


class ChangesResponse(ApiModel):
    """How the server applied a device's batch."""

    applied: int | float
    """How many changes the server applied."""
    skipped: int | float
    """How many changes the server skipped, such as the changes of a batch it had applied before,
    an update to a missing row, or a change that lost to the server's row.
    """
    conflicts: int | float
    """How many changes a conflict resolver settled."""


ReadConcernLevel: TypeAlias = Annotated[Literal["local", "majority", "linearizable"], OPEN_ENUM]
"""How current the data must be before a node serves a read."""


class ClusterReadEndpointInfo(ApiModel):
    """One node that a client can read from, and the read concerns that the node meets now."""

    node_id: str = Field(alias="nodeId")
    """The identifier of the node at this endpoint."""
    endpoint: str
    """The address that a client sends its reads to."""
    read_concerns: list[ReadConcernLevel] = Field(alias="readConcerns")
    """The read concerns that this node meets now."""


class ClusterStatusResponseCurrentPrimaryObject(ApiModel):
    """The shape of one form of the `currentPrimary` field of `ClusterStatusResponse`."""

    node_id: str = Field(alias="nodeId")
    """The identifier of the primary node."""
    endpoint: str | None = None
    """The address at which a client reaches the primary."""


class ClusterStatusResponse(ApiModel):
    """The body of `GET /db/{id}/cluster`, which states where a client sends the database's writes
    and reads.
    """

    database_id: str = Field(alias="databaseId")
    """The identifier of the database that this status describes."""
    replication_group_id: str | None = Field(default=None, alias="replicationGroupId")
    """The identifier of the node's replication group."""
    role: Annotated[Literal["primary", "replica"], OPEN_ENUM] | None = None
    """Whether this node is the primary, which accepts writes, or a replica, which serves reads."""
    current_primary: ClusterStatusResponseCurrentPrimaryObject | None = Field(default=None, alias="currentPrimary")
    """The primary that this node reports as current, or null when the node reports none."""
    read_endpoints: list[ClusterReadEndpointInfo] | None = Field(default=None, alias="readEndpoints")
    """Every node that a client can read from, with the read concerns that each one meets."""
    health: Annotated[Literal["degraded", "failing_over", "unavailable", "repairing", "syncing", "healthy"], OPEN_ENUM]
    """What this node can do now."""
    health_reason: Annotated[
        Literal[
            "repairing",
            "in-sync",
            "lagging",
            "coordinator-unreachable",
            "draining",
            "faulted",
            "not-member",
            "sync-pending",
            "no-group-state",
        ],
        OPEN_ENUM,
    ] = Field(alias="healthReason")
    """The condition behind that health state."""
    primary_term: str | None = Field(default=None, alias="primaryTerm")
    """The primary term that this node reports as current, as a decimal string so that a 64-bit
    term stays exact.
    """


class DatabaseStatus(ApiModel):
    """Whether one open database can serve requests."""

    id: str
    """The identifier under which the registry holds the database."""
    read_only: bool = Field(alias="readOnly")
    """Whether the database refuses writes."""
    closed: bool
    """Whether the database has closed, which makes the readiness status `degraded`."""


class EncryptionChangeRequest(ApiModel):
    """The body of `POST /db/{id}/encryption`, which encrypts the database, changes its key, or
    removes its encryption.
    """

    key_ref: str | None = Field(alias="keyRef")
    """The name, of 1 to 256 characters, that the server's `encryptionKeys` function turns into the
    target key, or null to remove encryption. No key crosses the wire.
    """


class EncryptionChangeStatusError(ApiModel):
    """The code and message of the error that stopped the change."""

    code: str
    """The machine-readable code of the error."""
    message: str
    """What went wrong, for a log or a person."""


class EncryptionChangeStatusReopenError(ApiModel):
    """The code and message of an error that came after the new file was in place, such as a key
    function that failed when the connections reopened. The change is `done` and the file holds
    ciphertext under the new key, so keep that key, and close and open the database again with
    it.
    """

    code: str
    """The machine-readable code of the error."""
    message: str
    """What went wrong, for a log or a person."""


class EncryptionChangeStatus(ApiModel):
    """The progress of the current key change, or the outcome of the last one."""

    state: Annotated[Literal["idle", "done", "failed", "copying", "catching-up", "swapping"], OPEN_ENUM]
    """`idle` before any change, `copying` while Sirannon copies a snapshot to the new file,
    `catching-up` while it applies the writes committed since that snapshot, `swapping` while it
    holds writes and replaces the file, and `done` or `failed` once the change ends.
    """
    target: Annotated[Literal["encrypted", "plaintext"], OPEN_ENUM] | None = None
    """Whether the change encrypts the file or stores it as plaintext."""
    started_at: int | float | None = Field(default=None, alias="startedAt")
    """The time at which the change started, in milliseconds since the Unix epoch."""
    finished_at: int | float | None = Field(default=None, alias="finishedAt")
    """The time at which the change ended, in milliseconds since the Unix epoch."""
    rows_copied: int | float | None = Field(default=None, alias="rowsCopied")
    """The number of rows that the snapshot copy wrote to the new file."""
    rounds: int | float | None = None
    """The number of rounds in which Sirannon applied the writes committed since the snapshot."""
    error: EncryptionChangeStatusError | None = None
    """The code and message of the error that stopped the change."""
    reopen_error: EncryptionChangeStatusReopenError | None = Field(default=None, alias="reopenError")
    """The code and message of an error that came after the new file was in place, such as a key
    function that failed when the connections reopened. The change is `done` and the file holds
    ciphertext under the new key, so keep that key, and close and open the database again with
    it.
    """


class EncryptionStatus(ApiModel):
    """Reports whether a database is encrypted, under which key, and how its last key change went."""

    encrypted: bool
    """Whether the database file holds ciphertext."""
    cipher: Annotated[Literal["aegis-256", "sqlcipher-4"], OPEN_ENUM] | None = None
    """The scheme that an encrypted file uses: `aegis-256` from the better-sqlite3 driver, or
    `sqlcipher-4` from the Expo driver.
    """
    cipher_version: str | None = Field(default=None, alias="cipherVersion")
    """The version of the encryption engine, which an encrypted database reports."""
    key_id: str | None = Field(default=None, alias="keyId")
    """The identifier that came with the current key."""
    changed_at: int | float | None = Field(default=None, alias="changedAt")
    """The time at which the last key change finished, in milliseconds since the Unix epoch."""
    change: EncryptionChangeStatus
    """The current key change, or the last one."""


class ErrorResponseError(ApiModel):
    """A machine-readable code, a human-readable message, and any details that the route adds."""

    code: str
    """The machine-readable code of the error, which the client passes to the caller unchanged."""
    message: str
    """What went wrong, for a log or a person."""
    details: dict[str, Any] | None = None
    """The routing context of the error, such as `currentPrimary`, `primaryTerm`, or
    `serverVersion`, present only when it holds a value.
    """


class ErrorResponse(ApiModel):
    """The body that every failed route returns."""

    error: ErrorResponseError
    """A machine-readable code, a human-readable message, and any details that the route adds."""


class ExecuteRequest(ApiModel):
    """Body of `POST /db/{id}/execute`."""

    sql: str
    """The statement to execute."""
    params: dict[str, Any] | list[Any] | None = None
    """The values to bind to the statement, by name or by position."""
    write_concern: WriteConcern | None = Field(default=None, alias="writeConcern")
    """The acknowledgements that the server waits for before it confirms this write."""


class IntegerEnvelope(ApiModel):
    """An integer outside the range from -(2^53 - 1) to 2^53 - 1, which JSON numbers cannot hold
    exactly. A row, a change event, a bind parameter, and a subscription filter carry such an
    integer in this envelope. Decode it into a 64-bit integer, and send an integer outside that
    range in the same envelope.
    """

    sirannon_int: str = Field(alias="__sirannon_int")
    """The integer in decimal, of 1 to 19 digits with an optional leading `-`."""


class LivenessResponse(ApiModel):
    """The body of `GET /health`, which the server sends while its process runs."""

    status: Literal["ok"]
    """Always `ok`, because a process that cannot answer sends nothing."""


class LoadRequest(ApiModel):
    """The body of `POST /db/{id}/load`, which loads rows while the server relaxes the writer's
    durability. The server restores the configured durability before it sends the response. When
    the process crashes during a load, SQLite rolls back the uncommitted rows, so the client can
    send the load again.
    """

    sql: str
    """The statement to execute for each parameter set."""
    params_batch: list[dict[str, Any] | list[Any]] = Field(alias="paramsBatch")
    """One parameter set per row."""
    durability: BulkLoadDurability | None = None
    """The writer's durability level during the load. Defaults to 'off'."""
    checkpoint: bool | None = None
    """Whether the server checkpoints the WAL after this load. Defaults to true, so set it to false
    on every batch except the last of a multi-batch import.
    """


class LoadResponse(ApiModel):
    """The totals for a bulk load, which Sirannon sums across rows so that a large load keeps no
    per-row results in memory.
    """

    rows_loaded: int | float = Field(alias="rowsLoaded")
    """The number of parameter sets that the load applied."""
    changes: int | float
    """The number of rows that the load inserted, updated, or deleted."""


class MigrationListEntry(ApiModel):
    """One migration that the database has applied."""

    version: int | float
    """The version of the migration."""
    name: str
    """The name of the migration."""
    checksum: str | None
    """The checksum of the migration's statements, or null when the database recorded none."""
    up: str | None = None
    """The statements of a migration later than `after`, which the server sends when it holds them.
    """


class MigrationListRequest(ApiModel):
    """The body of `POST /db/{id}/migrations`, which lists the migrations that the database has
    applied.
    """

    after: int | float | None = None
    """The schema version of the device, so that the server sends the statements of every later
    migration.
    """


class MigrationListResponse(ApiModel):
    """The migrations that the database has applied."""

    server_version: int | float = Field(alias="serverVersion")
    """The schema version of the server's database."""
    migrations: list[MigrationListEntry]
    """Every migration that the database has applied, in version order."""


class OperationExecuteRequest(ApiModel):
    """The body of `POST /db/{id}/execute/{name}`, which runs a registered write in one
    transaction.
    """

    args: dict[str, Any] | None = None
    """The arguments that the registered write takes, by name. The server fills the arguments that
    come from identity.
    """
    write_concern: WriteConcern | None = Field(default=None, alias="writeConcern")
    """The acknowledgements that the server waits for before it confirms this write."""


class OperationExecuteResponse(ApiModel):
    """The body that `POST /db/{id}/execute/{name}` returns."""

    results: list[ExecuteResponse]
    """One result per statement of the registered write, in the order that the transaction ran
    them.
    """


class ReadConcern(ApiModel):
    """How current the data must be before a node serves a read."""

    level: ReadConcernLevel
    """The level that the node must confirm before it responds."""


class OperationQueryRequest(ApiModel):
    """The body of `POST /db/{id}/query/{name}`, which runs a registered read."""

    args: dict[str, Any] | None = None
    """The arguments that the registered read takes, by name. The server fills the arguments that
    come from identity.
    """
    read_concern: ReadConcern | None = Field(default=None, alias="readConcern")
    """The read concern that this read requires."""


class QueryRequest(ApiModel):
    """Body of `POST /db/{id}/query`."""

    sql: str
    """The statement to execute."""
    params: dict[str, Any] | list[Any] | None = None
    """The values to bind to the statement, by name or by position."""
    read_concern: ReadConcern | None = Field(default=None, alias="readConcern")
    """The read concern that this read requires."""


class QueryResponse(ApiModel):
    """The body that a read route returns."""

    rows: list[dict[str, Any]]
    """The rows that the statement returned, with blobs and large integers in their tagged wire
    form.
    """


class ReplicationReadinessController(ApiModel):
    """The state of this node's failover controller."""

    state: Annotated[Literal["disabled", "standby", "active", "lost"], OPEN_ENUM]
    """Whether the controller is off, waiting, leading the group, or has lost its lease."""


class ReplicationReadinessCoordinator(ApiModel):
    """Whether this node reaches the coordinator, and whether it holds the primary authority."""

    connected: bool
    """Whether this node reaches the coordinator."""
    authority: bool
    """Whether this node holds the primary authority."""


class ReplicationReadiness(ApiModel):
    """The replication state of one node, as `GET /health/ready` reports it."""

    role: str
    """Whether this node is the primary or a replica."""
    write_forwarding: bool = Field(alias="writeForwarding")
    """Whether this node forwards the writes that it receives to the primary."""
    peers: int | float
    """How many peers this node replicates with."""
    local_seq: str = Field(alias="localSeq")
    """The highest change sequence that this node has stored, as a decimal string."""
    replication_group_id: str | None = Field(default=None, alias="replicationGroupId")
    """The identifier of this node's replication group."""
    primary_term: str | None = Field(default=None, alias="primaryTerm")
    """The primary term that this node reports as current, as a decimal string."""
    current_primary: str | None = Field(default=None, alias="currentPrimary")
    """The identifier of the node that this node reports as the current primary."""
    coordinator: ReplicationReadinessCoordinator | None = None
    """Whether this node reaches the coordinator, and whether it holds the primary authority."""
    controller: ReplicationReadinessController | None = None
    """The state of this node's failover controller."""
    in_sync_replicas: list[str] | None = Field(default=None, alias="inSyncReplicas")
    """The replicas that the group counts as in sync."""
    lagging_replicas: list[str] | None = Field(default=None, alias="laggingReplicas")
    """The replicas that have fallen behind."""
    sync_state: str | None = Field(default=None, alias="syncState")
    """How far this node has caught up with the primary."""
    health_reason: Annotated[
        Literal[
            "repairing",
            "in-sync",
            "lagging",
            "coordinator-unreachable",
            "draining",
            "faulted",
            "not-member",
            "sync-pending",
            "no-group-state",
        ],
        OPEN_ENUM,
    ] = Field(alias="healthReason")
    """The condition behind this node's health state."""
    read_availability: Annotated[Literal["unavailable", "available"], OPEN_ENUM] | None = Field(
        default=None, alias="readAvailability"
    )
    """Whether this node serves reads now."""
    write_availability: Annotated[Literal["unavailable", "available"], OPEN_ENUM] | None = Field(
        default=None, alias="writeAvailability"
    )
    """Whether this node serves writes now."""


class ReadinessResponse(ApiModel):
    """The body of `GET /health/ready`, which states whether the node can serve reads and writes."""

    status: Annotated[Literal["ok", "degraded", "failing_over", "unavailable", "repairing", "syncing"], OPEN_ENUM]
    """What the node can do now: `ok` when it is healthy, `degraded` when a database has closed,
    and the replication health state otherwise.
    """
    databases: list[DatabaseStatus]
    """One entry for each database that the registry holds open."""
    replication: ReplicationReadiness | None = None
    """The replication state of this node, which the server reports when it runs with replication.
    """


class SnapshotManifestMigration(ApiModel):
    """One migration that the database has applied."""

    version: int | float
    """The version of the migration."""
    name: str
    """The name of the migration."""
    checksum: str | None
    """The checksum of the migration's statements, or null when the database recorded none."""


class SnapshotManifestTable(ApiModel):
    """One table that a snapshot copies."""

    name: str
    """The name of the table."""
    row_count: int | float = Field(alias="rowCount")
    """How many rows the table held when the server wrote the manifest."""


class SnapshotManifestResponse(ApiModel):
    """The body of `POST /db/{id}/snapshot`, which describes everything a device needs to copy the
    database.
    """

    database_id: str = Field(alias="databaseId")
    """The identifier of the database that the snapshot copies."""
    start_seq: str = Field(alias="startSeq")
    """The change sequence from which a device subscribes once it has copied every page, as a
    decimal string.
    """
    epoch: str
    """The identifier of the database's change sequence space."""
    schema_: list[str] = Field(alias="schema")
    """The statements that create the database's tables, in foreign key order, followed by its
    other schema objects such as indexes.
    """
    tables: list[SnapshotManifestTable]
    """Each table to copy, in the order that its foreign keys need, with its row count."""
    migrations: list[SnapshotManifestMigration]
    """The migrations that the database has applied."""


class SnapshotPageRequest(ApiModel):
    """The body of `POST /db/{id}/snapshot/page`, which asks for one page of a table's rows."""

    table: str
    """The table to read."""
    after_key: list[Any] | None = Field(default=None, alias="afterKey")
    """The key of the last row of the previous page, which the server reads past. Leave it out for
    the first page.
    """
    limit: int | float | None = None
    """The most rows to return, from 1 to 1000. Defaults to 500."""


class SnapshotPageResponse(ApiModel):
    """One page of a table's rows, with each column value in its tagged wire form."""

    rows: list[Any]
    """The rows of this page."""
    checksum: str
    """The checksum over the rows of this page."""
    done: bool
    """Whether this is the table's last page."""
    next_key: list[Any] | None = Field(alias="nextKey")
    """The key to send as `afterKey` for the next page, or null on the last page."""


class StartedResponse(ApiModel):
    """The server's answer when it has started work that finishes after it replies, such as a
    restore or a key change.
    """

    started: Literal[True]
    """Always true, because the server sends this answer only once the work has started."""


class TransactionStatement(ApiModel):
    """One statement inside a transaction or a registered write."""

    sql: str
    """The statement to execute."""
    params: dict[str, Any] | list[Any] | None = None
    """The values to bind to the statement, by name or by position."""


class TransactionRequest(ApiModel):
    """Body of `POST /db/{id}/transaction`, whose statements all succeed or all fail."""

    statements: list[TransactionStatement]
    """The statements to execute, in order."""
    write_concern: WriteConcern | None = Field(default=None, alias="writeConcern")
    """The acknowledgements that the server waits for before it confirms the transaction."""


class TransactionResponse(ApiModel):
    """The body that a transaction route returns."""

    results: list[ExecuteResponse]
    """One result per statement, in the order that the transaction executed them."""


class AckResponse(ApiModel):
    """The server's answer to a device's acknowledgement."""

    acked: bool
    """True, because the server answers only once it has recorded the acknowledgement."""
    seq: str
    """The sequence that the device acknowledged, as a decimal string."""


class BulkLoadResult(ApiModel):
    """The totals for a bulk load, which Sirannon sums across rows so that a large load keeps no
    per-row results in memory.
    """

    rows_loaded: int | float = Field(alias="rowsLoaded")
    """The number of parameter sets that the load applied."""
    changes: int | float
    """The number of rows that the load inserted, updated, or deleted."""


class WSAckMessage(ApiModel):
    """A device's acknowledgement of every change that it has stored up to a sequence."""

    type: Literal["ack"]
    """The message type for an acknowledgement."""
    id: str
    """The identifier that the client chooses, which the server repeats in its reply."""
    device_id: str = Field(alias="deviceId")
    """The identifier of the device that sends the acknowledgement."""
    seq: str
    """The highest sequence that the device has stored, as a decimal string."""


class WSBatchMessage(ApiModel):
    """A request to execute one statement with many parameter sets in a single server-side
    transaction.
    """

    type: Literal["batch"]
    """The message type for a batch."""
    id: str
    """The identifier that the client chooses, which the server repeats in its reply."""
    sql: str
    """The statement to execute for each parameter set."""
    params_batch: list[dict[str, Any] | list[Any]] = Field(alias="paramsBatch")
    """One parameter set for each execution of the statement."""
    write_concern: WriteConcern | None = Field(default=None, alias="writeConcern")
    """The acknowledgements that the server waits for before it confirms the batch."""


class WSWireChangeEvent(ApiModel):
    """One change event in its wire form, with the sequence as a decimal string, so that JSON keeps
    a value beyond the safe integer range exact.
    """

    type: Annotated[Literal["insert", "update", "delete"], OPEN_ENUM]
    """Whether the row was inserted, updated, or deleted."""
    table: str
    """The table of the row."""
    row: dict[str, Any]
    """The row's values after the change."""
    old_row: dict[str, Any] | None = Field(default=None, alias="oldRow")
    """The row's values before an update or a delete."""
    seq: str
    """The position of this change in the database's change log, as a decimal string."""
    timestamp: int | float
    """Milliseconds since the Unix epoch, from when the database recorded the change."""
    hlc: str | None = None
    """The hybrid logical clock stamp that the writing node gave this change."""
    origin: str | None = None
    """The identifier of the node or device that wrote the change."""
    row_id: str | None = Field(default=None, alias="rowId")
    """The primary key of the changed row, encoded as a string."""
    tx_id: str | None = Field(default=None, alias="txId")
    """The identifier of the transaction that made this change."""
    tx_end: bool | None = Field(default=None, alias="txEnd")
    """Set on the last change of a transaction."""


class WSChangeMessage(ApiModel):
    """One change event for a subscriber."""

    type: Literal["change"]
    """The message type for a single change event."""
    id: str
    """The identifier of the subscription that this change is for."""
    event: WSWireChangeEvent
    """The change itself."""


class WSChangesMessage(ApiModel):
    """Several change events in one frame, in ascending seq order. The server sends this frame only
    on a device subscription that asks for `stagedStream`, and each event has the same fields as
    the event in a `change` frame.
    """

    type: Literal["changes"]
    """The message type for several change events."""
    id: str
    """The identifier of the subscription that these changes are for."""
    events: list[WSWireChangeEvent]
    """The changes, in ascending sequence order."""


class WSErrorMessageError(ApiModel):
    """A machine-readable code and a human-readable message."""

    code: str
    """The machine-readable code of the error, which the client passes to the caller unchanged."""
    message: str
    """What went wrong, for a log or a person."""


class WSErrorMessage(ApiModel):
    """The server's report that a request failed."""

    type: Literal["error"]
    """The message type for a failure."""
    id: str
    """The identifier from the request, or an empty string when the server cannot read one."""
    error: WSErrorMessageError
    """A machine-readable code and a human-readable message."""


class WSExecuteMessage(ApiModel):
    """A request to execute a write, either as SQL or by the name of a registered write."""

    type: Literal["execute"]
    """The message type for a write."""
    id: str
    """The identifier that the client chooses, which the server repeats in its reply."""
    sql: str | None = None
    """The statement to execute, which the server executes only when it accepts SQL."""
    params: dict[str, Any] | list[Any] | None = None
    """The values to bind to that statement, by name or by position."""
    name: str | None = None
    """The name of a registered write to execute in place of `sql`."""
    args: dict[str, Any] | None = None
    """The arguments that the registered write takes."""
    write_concern: WriteConcern | None = Field(default=None, alias="writeConcern")
    """The acknowledgements that the server waits for before it confirms this write."""


class WSLiveOpVariant1(ApiModel):
    """The shape of one form of `WSLiveOp`."""

    op: Literal["insert"]
    """Which edit this is: `'insert'` for a row added, `'update'` for a row changed in place,
    `'delete'` for a row removed.
    """
    index: int | float
    """The zero-based position in the result set that the edit applies to."""
    row: Any
    """The row that is now at that position."""


class WSLiveOpVariant2(ApiModel):
    """The shape of one form of `WSLiveOp`."""

    op: Literal["update"]
    """The edit type for a row changed in place."""
    index: int | float
    """The position of the row that changed."""
    row: Any
    """The row's new values."""


class WSLiveOpVariant3(ApiModel):
    """The shape of one form of `WSLiveOp`."""

    op: Literal["delete"]
    """The edit type for a removed row."""
    index: int | float
    """The position of the removed row."""


WSLiveOp: TypeAlias = WSLiveOpVariant1 | WSLiveOpVariant2 | WSLiveOpVariant3
"""One edit to a live query's result set, as a position and the row at that position."""


class WSLiveMessage(ApiModel):
    """A live query's new state, which is the edits that move it, a full replacement, or a notice
    that the server has started to read the query again.
    """

    type: Literal["live"]
    """The message type for a live-query update."""
    id: str
    """The identifier of the subscription that this update is for."""
    ops: list[WSLiveOp] | None = None
    """The edits that move the result set to its new state."""
    rows: list[Any] | None = None
    """A complete replacement result set."""
    revalidating: bool | None = None
    """Set while the server reads the query again."""


class WSLoadMessage(ApiModel):
    """A request to import many rows at relaxed durability, which the server restores before it
    replies.
    """

    type: Literal["load"]
    """The message type for a load."""
    id: str
    """The identifier that the client chooses, which the server repeats in its reply."""
    sql: str
    """The statement to execute for each parameter set."""
    params_batch: list[dict[str, Any] | list[Any]] = Field(alias="paramsBatch")
    """One parameter set per row."""
    durability: Annotated[Literal["off", "normal"], OPEN_ENUM] | None = None
    """The writer's durability level during the load. Defaults to 'off'."""
    checkpoint: bool | None = None
    """Whether the server checkpoints the WAL after this load. Defaults to true."""


class WSQueryMessage(ApiModel):
    """A request to execute a read, either as SQL or by the name of a registered read."""

    type: Literal["query"]
    """The message type for a read."""
    id: str
    """The identifier that the client chooses, which the server repeats in its reply."""
    sql: str | None = None
    """The statement to execute, which the server executes only when it accepts SQL."""
    params: dict[str, Any] | list[Any] | None = None
    """The values to bind to that statement, by name or by position."""
    name: str | None = None
    """The name of a registered read to execute in place of `sql`."""
    args: dict[str, Any] | None = None
    """The arguments that the registered read takes."""
    read_concern: ReadConcern | None = Field(default=None, alias="readConcern")
    """The read concern that this read requires."""


class WSResultMessage(ApiModel):
    """The server's reply to a read, write, transaction, batch, load, or acknowledgement."""

    type: Literal["result"]
    """The message type for a reply to a read, write, transaction, batch, load, or acknowledgement.
    """
    id: str
    """The identifier from the request."""
    data: QueryResponse | ExecuteResponse | TransactionResponse | BatchResponse | BulkLoadResult | AckResponse
    """The reply body, whose shape depends on the request."""


class WSSubscribedMessage(ApiModel):
    """The server's confirmation that a subscription is open, with the cursor and the epoch that it
    streams from.
    """

    type: Literal["subscribed"]
    """The message type for a subscription confirmation."""
    id: str
    """The identifier of the subscription that is now open."""
    max_unacknowledged_changes: int | float | None = Field(default=None, alias="maxUnacknowledgedChanges")
    """The number of changes that the server sends past the device's acknowledged cursor before it
    pauses delivery. The server sends it only for a device subscription.
    """
    seq: str | None = None
    """The seq from which the subscription is live. A client that has not yet received any change
    stores this seq as its resume cursor, so that after a reconnect during a quiet period, the
    server still replays every change that the client missed.
    """
    resync: bool | None = None
    """Set when the server cannot replay from the requested `sinceSeq`, because that seq is older
    than the history that the server retains, the client's `epoch` differs from the server's, or
    the replay fails. The server skips the replay and the subscription starts from the server's
    current position, so the client must treat its earlier state as stale and read it again.
    """
    epoch: str | None = None
    """The identifier of the sequence space that this subscription streams from. The client stores
    it and sends it back when it resumes, so that the server answers a cursor from a different
    database with `resync: true`.
    """
    rows: list[Any] | None = None
    """The first result set of a live query, which the server sends when the subscription names a
    registered read.
    """


class WSSubscribeMessage(ApiModel):
    """A request to open a change subscription on one or more tables, or a live query on a
    registered read.
    """

    type: Literal["subscribe"]
    """The message type for a subscribe."""
    id: str
    """The identifier that the client chooses, which the server repeats in its replies and change
    events.
    """
    table: str | None = None
    """The table to subscribe to."""
    tables: list[str] | None = None
    """Several tables to subscribe to at once."""
    filter: dict[str, Any] | None = None
    """Narrows the subscription to rows whose columns equal these values."""
    name: str | None = None
    """The name of a registered read, so that the server opens a live query on that read."""
    args: dict[str, Any] | None = None
    """The arguments that the registered read takes."""
    registry_digest: str | None = Field(default=None, alias="registryDigest")
    """The digest of the operation registry that the client built its live query against."""
    since_seq: str | None = Field(default=None, alias="sinceSeq")
    """The highest `seq` that the client has processed. When it is present, the server replays
    every retained change with a greater seq before it sends live events, so that a reconnecting
    subscriber receives the changes that it missed. When the server answers with `resync: true`,
    it skips the replay. The client sends the value as a decimal string, so that a value beyond
    `Number.MAX_SAFE_INTEGER` stays exact.
    """
    epoch: str | None = None
    """The `epoch` that the server reported with this cursor. A `sinceSeq` is valid only within the
    sequence space that produced it, so when this epoch differs from the server's own, the
    server answers with `resync: true` and skips the replay.
    """
    device_id: str | None = Field(default=None, alias="deviceId")
    """The device's identifier, which makes this a device-sync subscription."""
    schema_version: int | float | None = Field(default=None, alias="schemaVersion")
    """The schema version of the device's local database, which the server checks before it opens
    the stream.
    """
    staged_stream: bool | None = Field(default=None, alias="stagedStream")
    """Set to true when this device stages pulled changes durably and acknowledges staged
    sequences. The server then packs several events into each `changes` frame and can pause the
    stream at any event. The server accepts it only with `deviceId`.
    """


class WSTransactionMessage(ApiModel):
    """A request to execute every statement in one server-side transaction, to which the server
    replies once with all the results. The server makes no round trip to the client between
    statements, so it holds the single writer lock only while the statements execute.
    """

    type: Literal["transaction"]
    """The message type for a transaction."""
    id: str
    """The identifier that the client chooses, which the server repeats in its reply."""
    statements: list[TransactionStatement]
    """The statements to execute, in order."""
    write_concern: WriteConcern | None = Field(default=None, alias="writeConcern")
    """The acknowledgements that the server waits for before it confirms the transaction."""


class WSUnsubscribedMessage(ApiModel):
    """The server's confirmation that a subscription has ended."""

    type: Literal["unsubscribed"]
    """The message type for an unsubscribe confirmation."""
    id: str
    """The identifier of the subscription that has ended."""


class WSUnsubscribeMessage(ApiModel):
    """A request to end a subscription."""

    type: Literal["unsubscribe"]
    """The message type for an unsubscribe."""
    id: str
    """The identifier of the subscription to end."""


__all__ = [
    "AckResponse",
    "BackupChain",
    "BackupChainBase",
    "BackupChainChange",
    "BackupChainPosition",
    "BackupChainRecord",
    "BackupChainResponse",
    "BackupCycleError",
    "BackupLogPosition",
    "BackupProgress",
    "BackupRestoreProgress",
    "BackupRestoreRequest",
    "BackupRestoreState",
    "BackupRestoreStatus",
    "BackupRestoreStatusError",
    "BackupRestoreStatusReopenError",
    "BackupRestoreSummary",
    "BackupRunSummary",
    "BackupSafeToDeleteRequest",
    "BackupSafeToDeleteResponse",
    "BackupSkip",
    "BackupSkipReason",
    "BackupStatusResponse",
    "BackupTriggerResponse",
    "BackupVerifyRequest",
    "BackupVerifyResponse",
    "BatchRequest",
    "BatchResponse",
    "BlobEnvelope",
    "BulkLoadDurability",
    "BulkLoadResult",
    "CapabilitiesResponse",
    "CapabilitiesResponseRegistry",
    "ChangesRequest",
    "ChangesResponse",
    "ClusterReadEndpointInfo",
    "ClusterStatusResponse",
    "ClusterStatusResponseCurrentPrimaryObject",
    "DatabaseStatus",
    "EncryptionChangeRequest",
    "EncryptionChangeStatus",
    "EncryptionChangeStatusError",
    "EncryptionChangeStatusReopenError",
    "EncryptionStatus",
    "ErrorResponse",
    "ErrorResponseError",
    "ExecuteRequest",
    "ExecuteResponse",
    "IntegerEnvelope",
    "LivenessResponse",
    "LoadRequest",
    "LoadResponse",
    "MigrationListEntry",
    "MigrationListRequest",
    "MigrationListResponse",
    "OperationExecuteRequest",
    "OperationExecuteResponse",
    "OperationQueryRequest",
    "QueryRequest",
    "QueryResponse",
    "ReadConcern",
    "ReadConcernLevel",
    "ReadinessResponse",
    "ReplicationReadiness",
    "ReplicationReadinessController",
    "ReplicationReadinessCoordinator",
    "SnapshotManifestMigration",
    "SnapshotManifestResponse",
    "SnapshotManifestTable",
    "SnapshotPageRequest",
    "SnapshotPageResponse",
    "StartedResponse",
    "SyncBatch",
    "SyncBatchHlcRange",
    "SyncChange",
    "TransactionRequest",
    "TransactionResponse",
    "TransactionStatement",
    "WSAckMessage",
    "WSBatchMessage",
    "WSChangeMessage",
    "WSChangesMessage",
    "WSErrorMessage",
    "WSErrorMessageError",
    "WSExecuteMessage",
    "WSLiveMessage",
    "WSLiveOp",
    "WSLiveOpVariant1",
    "WSLiveOpVariant2",
    "WSLiveOpVariant3",
    "WSLoadMessage",
    "WSQueryMessage",
    "WSResultMessage",
    "WSSubscribeMessage",
    "WSSubscribedMessage",
    "WSTransactionMessage",
    "WSUnsubscribeMessage",
    "WSUnsubscribedMessage",
    "WSWireChangeEvent",
    "WriteConcern",
    "WriteConcernLevel",
]
