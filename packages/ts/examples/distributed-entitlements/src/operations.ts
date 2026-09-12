import { randomUUID } from 'node:crypto'
import type { OperationRegistry } from '@delali/sirannon-db'
import { SEED_CUSTOMERS } from './cluster-node-schema'
import {
  CUSTOMER_STATUSES,
  MAX_API_QUOTA,
  MAX_SEATS,
  MAX_USAGE_UNITS,
  MAX_VERSION,
  PLANS,
  readActor,
  readChoice,
  readCount,
  readFlag,
  readText,
  SUPPORT_TIERS,
  toExternalId,
} from './operation-arguments'
import {
  AUDIT_LOG_SQL,
  BILLING_EVENTS_SQL,
  CUSTOMER_ENTITLEMENTS_SQL,
  DECREMENT_USAGE_QUOTA_SQL,
  DELETE_AUDIT_LOG_SQL,
  DELETE_BILLING_EVENTS_SQL,
  DELETE_CUSTOMERS_SQL,
  DELETE_ENTITLEMENTS_SQL,
  DELETE_USAGE_EVENTS_SQL,
  FINALIZE_BILLING_EVENT_SQL,
  INSERT_AUDIT_SQL,
  INSERT_BILLING_AUDIT_SQL,
  INSERT_BILLING_EVENT_SQL,
  INSERT_CUSTOMER_SQL,
  INSERT_ENTITLEMENT_SQL,
  INSERT_USAGE_AUDIT_SQL,
  INSERT_USAGE_EVENT_SQL,
  UPDATE_CUSTOMER_FROM_BILLING_SQL,
  UPDATE_ENTITLEMENT_FROM_BILLING_SQL,
  USAGE_EVENTS_SQL,
} from './operations-sql'

export interface ControlPlaneOperator {
  actor: string
}

const NEW_CUSTOMER_BILLING_VERSION = 1

export const operations = {
  entitlements: {
    reads: {
      customerEntitlements: {
        columns: [
          'id',
          'external_id',
          'name',
          'plan',
          'status',
          'created_at',
          'seats',
          'api_quota',
          'support_tier',
          'active',
          'version',
          'updated_at',
        ],
        statement: () => ({ sql: CUSTOMER_ENTITLEMENTS_SQL }),
      },
      usageEvents: {
        columns: ['id', 'customer_id', 'customer_name', 'units', 'source', 'idempotency_key', 'created_at'],
        statement: () => ({ sql: USAGE_EVENTS_SQL }),
      },
      billingEvents: {
        columns: [
          'id',
          'provider_event_id',
          'event_type',
          'customer_external_id',
          'version',
          'outcome',
          'processed_at',
        ],
        statement: () => ({ sql: BILLING_EVENTS_SQL }),
      },
      auditLog: {
        columns: ['id', 'actor', 'action', 'target', 'detail', 'created_at'],
        statement: () => ({ sql: AUDIT_LOG_SQL }),
      },
    },
    writes: {
      createCustomer: {
        args: ['name', 'plan', 'seats', 'apiQuota', 'supportTier'],
        fromIdentity: { actor: 'actor' },
        statements: args => {
          const actor = readActor(args.actor)
          const name = readText(args.name, 'name')
          const plan = readChoice(args.plan, 'plan', PLANS)
          const seats = readCount(args.seats, 'seats', MAX_SEATS)
          const apiQuota = readCount(args.apiQuota, 'apiQuota', MAX_API_QUOTA)
          const supportTier = readChoice(args.supportTier, 'supportTier', SUPPORT_TIERS)
          const externalId = toExternalId(name, randomUUID().slice(0, 8))

          return [
            { sql: INSERT_CUSTOMER_SQL, params: [externalId, name, plan, 'active'] },
            {
              sql: INSERT_ENTITLEMENT_SQL,
              params: [seats, apiQuota, supportTier, NEW_CUSTOMER_BILLING_VERSION, externalId],
            },
            {
              sql: INSERT_AUDIT_SQL,
              params: [actor, 'customer_created', externalId, `Created ${name} with ${plan} entitlements`],
            },
          ]
        },
      },
      recordUsage: {
        args: ['customerId', 'customerName', 'units', 'source', 'idempotencyKey'],
        fromIdentity: { actor: 'actor' },
        statements: args => {
          const actor = readActor(args.actor)
          const customerId = readCount(args.customerId, 'customerId', Number.MAX_SAFE_INTEGER, 1)
          const customerName = readText(args.customerName, 'customerName')
          const units = readCount(args.units, 'units', MAX_USAGE_UNITS, 1)
          const source = readText(args.source, 'source')
          const idempotencyKey = readText(args.idempotencyKey, 'idempotencyKey')

          return [
            { sql: INSERT_USAGE_EVENT_SQL, params: [customerId, units, source, idempotencyKey] },
            { sql: DECREMENT_USAGE_QUOTA_SQL, params: [units, customerId] },
            {
              sql: INSERT_USAGE_AUDIT_SQL,
              params: [
                actor,
                'usage_recorded',
                String(customerId),
                `Recorded ${units} units for ${customerName} from ${source}`,
              ],
            },
          ]
        },
      },
      applyBillingEvent: {
        args: [
          'providerEventId',
          'eventType',
          'customerExternalId',
          'customerName',
          'version',
          'plan',
          'status',
          'seats',
          'apiQuota',
          'supportTier',
          'active',
        ],
        fromIdentity: { actor: 'actor' },
        statements: args => {
          const actor = readActor(args.actor)
          const providerEventId = readText(args.providerEventId, 'providerEventId')
          const eventType = readText(args.eventType, 'eventType')
          const customerExternalId = readText(args.customerExternalId, 'customerExternalId')
          const customerName = readText(args.customerName, 'customerName')
          const version = readCount(args.version, 'version', MAX_VERSION, 1)
          const plan = readChoice(args.plan, 'plan', PLANS)
          const status = readChoice(args.status, 'status', CUSTOMER_STATUSES)
          const seats = readCount(args.seats, 'seats', MAX_SEATS)
          const apiQuota = readCount(args.apiQuota, 'apiQuota', MAX_API_QUOTA)
          const supportTier = readChoice(args.supportTier, 'supportTier', SUPPORT_TIERS)
          const active = readFlag(args.active, 'active')
          const payload = JSON.stringify({ plan, seats, apiQuota, supportTier, active: active === 1 })

          return [
            {
              sql: INSERT_BILLING_EVENT_SQL,
              params: [providerEventId, eventType, customerExternalId, version, payload],
            },
            {
              sql: UPDATE_CUSTOMER_FROM_BILLING_SQL,
              params: [plan, status, customerExternalId, version, providerEventId],
            },
            {
              sql: UPDATE_ENTITLEMENT_FROM_BILLING_SQL,
              params: [seats, apiQuota, supportTier, active, version, customerExternalId, version, providerEventId],
            },
            { sql: FINALIZE_BILLING_EVENT_SQL, params: [providerEventId] },
            {
              sql: INSERT_BILLING_AUDIT_SQL,
              params: [
                actor,
                'billing_event_applied',
                customerExternalId,
                `${eventType} updated ${customerName} to version ${version}`,
                providerEventId,
              ],
            },
          ]
        },
      },
      resetControlPlane: {
        fromIdentity: { actor: 'actor' },
        statements: args => {
          const actor = readActor(args.actor)

          return [
            { sql: DELETE_BILLING_EVENTS_SQL },
            { sql: DELETE_USAGE_EVENTS_SQL },
            { sql: DELETE_AUDIT_LOG_SQL },
            { sql: DELETE_ENTITLEMENTS_SQL },
            { sql: DELETE_CUSTOMERS_SQL },
            ...SEED_CUSTOMERS.flatMap(customer => [
              {
                sql: INSERT_CUSTOMER_SQL,
                params: [customer.externalId, customer.name, customer.plan, customer.status],
              },
              {
                sql: INSERT_ENTITLEMENT_SQL,
                params: [
                  customer.seats,
                  customer.apiQuota,
                  customer.supportTier,
                  customer.version,
                  customer.externalId,
                ],
              },
            ]),
            {
              sql: INSERT_AUDIT_SQL,
              params: [actor, 'reset', 'control-plane', 'Reset entitlements to the seeded control-plane state'],
            },
          ]
        },
      },
    },
  },
} satisfies OperationRegistry<ControlPlaneOperator>
