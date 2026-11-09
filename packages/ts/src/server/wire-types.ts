import type { EncryptionStatus } from '../core/encryption/types.js'
import type { BulkLoadDurability, ReadConcern, WriteConcern } from '../core/types.js'
import type {
  BackupChainResponse,
  BackupRestoreRequest,
  BackupSafeToDeleteRequest,
  BackupSafeToDeleteResponse,
  BackupStatusResponse,
  BackupTriggerResponse,
  BackupVerifyRequest,
  BackupVerifyResponse,
} from './backup-protocol.js'
import type { BackupRestoreStatus } from './backup-restore-runs.js'
import type { CapabilitiesResponse } from './capabilities.js'
import type { LivenessResponse, ReadinessResponse } from './health.js'
import type { MigrationListRequest, MigrationListResponse } from './http-migrations.js'
import type {
  BatchRequest,
  BatchResponse,
  ClusterStatusResponse,
  ErrorResponse,
  ExecuteRequest,
  ExecuteResponse,
  LoadRequest,
  LoadResponse,
  QueryRequest,
  QueryResponse,
  TransactionRequest,
  TransactionResponse,
} from './protocol.js'
import type { SnapshotManifestResponse, SnapshotPageRequest, SnapshotPageResponse } from './snapshot-protocol.js'
import type { ChangesRequest, ChangesResponse } from './sync-protocol.js'
import type {
  WSAckMessage,
  WSBatchMessage,
  WSExecuteMessage,
  WSLoadMessage,
  WSQueryMessage,
  WSSubscribeMessage,
  WSTransactionMessage,
  WSUnsubscribeMessage,
} from './ws-protocol.js'
import type {
  WSChangeMessage,
  WSChangesMessage,
  WSErrorMessage,
  WSLiveMessage,
  WSResultMessage,
  WSSubscribedMessage,
  WSUnsubscribedMessage,
} from './ws-server-messages.js'

/**
 * An integer outside the range from -(2^53 - 1) to 2^53 - 1, which JSON numbers cannot hold exactly.
 *
 * The server and the client put such an integer in this envelope wherever it appears in a row, a change event, a bind
 * parameter, or a subscription filter. Decode it into a 64-bit integer, and send an integer outside that range in the
 * same envelope.
 *
 * @public
 */
export interface IntegerEnvelope {
  /** The integer in decimal, of 1 to 19 digits with an optional leading `-`. */
  __sirannon_int: string
}

/**
 * A BLOB, which JSON has no type for.
 *
 * The server and the client put every BLOB in this envelope wherever it appears in a row, a change event, a bind
 * parameter, or a subscription filter. Decode it into a byte array, and send a byte array in the same envelope.
 *
 * @public
 */
export interface BlobEnvelope {
  /** The bytes in uppercase hexadecimal, two digits per byte, where an empty string is an empty BLOB. */
  __sirannon_blob: string
}

/**
 * The body of `POST /db/{id}/query/{name}`, the route that executes a registered read.
 *
 * @public
 */
export interface OperationQueryRequest {
  /** The arguments that the registered read takes, by name. The server fills the arguments that come from identity. */
  args?: Record<string, unknown>
  /** The read concern that this read requires. */
  readConcern?: ReadConcern
}

/**
 * The body of `POST /db/{id}/execute/{name}`, the route that executes a registered write in one transaction.
 *
 * @public
 */
export interface OperationExecuteRequest {
  /** The arguments that the registered write takes, by name. The server fills the arguments that come from identity. */
  args?: Record<string, unknown>
  /** The acknowledgements that the server waits for before it confirms this write. */
  writeConcern?: WriteConcern
}

/**
 * The body that `POST /db/{id}/execute/{name}` returns.
 *
 * @public
 */
export interface OperationExecuteResponse {
  /** One result per statement of the registered write, in the order in which the transaction executed them. */
  results: ExecuteResponse[]
}

/**
 * The body of `POST /db/{id}/encryption`, the route that encrypts the database, changes its key, or removes its
 * encryption.
 *
 * @public
 */
export interface EncryptionChangeRequest {
  /**
   * The name, of 1 to 256 characters, that the server's `encryptionKeys` function turns into the target key, or null
   * to remove encryption. The request holds the name only, so only the server holds the key.
   */
  keyRef: string | null
}

/**
 * The server's answer when it has started work that finishes after it replies, such as a restore or a key change.
 *
 * @public
 */
export interface StartedResponse {
  /** This is always true, because the server sends this answer only once the work has started. */
  started: true
}

/**
 * Every type that a request or a response of the HTTP API holds, keyed by the schema name that the OpenAPI file gives
 * it.
 *
 * @internal
 */
export interface HttpWireTypes {
  ErrorResponse: ErrorResponse
  IntegerEnvelope: IntegerEnvelope
  BlobEnvelope: BlobEnvelope
  CapabilitiesResponse: CapabilitiesResponse
  LivenessResponse: LivenessResponse
  ReadinessResponse: ReadinessResponse
  ClusterStatusResponse: ClusterStatusResponse
  QueryRequest: QueryRequest
  QueryResponse: QueryResponse
  ExecuteRequest: ExecuteRequest
  ExecuteResponse: ExecuteResponse
  TransactionRequest: TransactionRequest
  TransactionResponse: TransactionResponse
  BatchRequest: BatchRequest
  BatchResponse: BatchResponse
  LoadRequest: LoadRequest
  LoadResponse: LoadResponse
  BulkLoadDurability: BulkLoadDurability
  OperationQueryRequest: OperationQueryRequest
  OperationExecuteRequest: OperationExecuteRequest
  OperationExecuteResponse: OperationExecuteResponse
  BackupTriggerResponse: BackupTriggerResponse
  BackupStatusResponse: BackupStatusResponse
  BackupChainResponse: BackupChainResponse
  BackupVerifyRequest: BackupVerifyRequest
  BackupVerifyResponse: BackupVerifyResponse
  BackupSafeToDeleteRequest: BackupSafeToDeleteRequest
  BackupSafeToDeleteResponse: BackupSafeToDeleteResponse
  BackupRestoreRequest: BackupRestoreRequest
  BackupRestoreStatus: BackupRestoreStatus
  StartedResponse: StartedResponse
  EncryptionChangeRequest: EncryptionChangeRequest
  EncryptionStatus: EncryptionStatus
  ChangesRequest: ChangesRequest
  ChangesResponse: ChangesResponse
  MigrationListRequest: MigrationListRequest
  MigrationListResponse: MigrationListResponse
  SnapshotManifestResponse: SnapshotManifestResponse
  SnapshotPageRequest: SnapshotPageRequest
  SnapshotPageResponse: SnapshotPageResponse
}

/**
 * The name of a schema that the OpenAPI file holds.
 *
 * @internal
 */
export type HttpWireTypeName = keyof HttpWireTypes

/**
 * Every message of the WebSocket protocol, keyed by the schema name that the AsyncAPI file gives it.
 *
 * @internal
 */
export interface WebSocketWireTypes {
  IntegerEnvelope: IntegerEnvelope
  BlobEnvelope: BlobEnvelope
  WSSubscribeMessage: WSSubscribeMessage
  WSUnsubscribeMessage: WSUnsubscribeMessage
  WSAckMessage: WSAckMessage
  WSQueryMessage: WSQueryMessage
  WSExecuteMessage: WSExecuteMessage
  WSTransactionMessage: WSTransactionMessage
  WSBatchMessage: WSBatchMessage
  WSLoadMessage: WSLoadMessage
  WSSubscribedMessage: WSSubscribedMessage
  WSUnsubscribedMessage: WSUnsubscribedMessage
  WSChangeMessage: WSChangeMessage
  WSChangesMessage: WSChangesMessage
  WSLiveMessage: WSLiveMessage
  WSResultMessage: WSResultMessage
  WSErrorMessage: WSErrorMessage
}

/**
 * The name of a schema that the AsyncAPI file holds.
 *
 * @internal
 */
export type WebSocketWireTypeName = keyof WebSocketWireTypes
