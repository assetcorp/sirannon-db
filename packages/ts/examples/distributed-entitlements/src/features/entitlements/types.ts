import type {
  ApplyBillingEventInput,
  AuditRecord,
  BillingEvent,
  ClusterNode,
  ControlPlaneSnapshot,
  CustomerEntitlement,
  RecordUsageInput,
  UsageEvent,
} from '../../lib/schemas'

export type ConnectionState = 'connecting' | 'live' | 'offline'

export interface LoaderData extends ControlPlaneSnapshot {
  initialError: string | null
}

export interface ControlPlaneStats {
  activeCustomers: number
  totalSeats: number
  totalQuota: number
  primaryNode: string
  unavailableNodes: number
}

export interface ControllerState {
  customers: CustomerEntitlement[]
  usage: UsageEvent[]
  billingEvents: BillingEvent[]
  auditLog: AuditRecord[]
  clusterNodes: ClusterNode[]
}

export interface UsageDraft extends Pick<RecordUsageInput, 'units' | 'source'> {
  idempotencyKey: string
}

export type BillingDraft = Omit<ApplyBillingEventInput, 'customerExternalId' | 'customerName'>

export const EMPTY_CONTROL_PLANE: ControlPlaneSnapshot = {
  customers: [],
  usage: [],
  billingEvents: [],
  auditLog: [],
  clusterNodes: [],
}
