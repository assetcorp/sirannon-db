export const CUSTOMER_ENTITLEMENTS_SQL = `
  SELECT
    customers.id,
    customers.external_id,
    customers.name,
    customers.plan,
    customers.status,
    customers.created_at,
    entitlements.seats,
    entitlements.api_quota,
    entitlements.support_tier,
    entitlements.active,
    entitlements.version,
    entitlements.updated_at
  FROM customers
  INNER JOIN entitlements ON entitlements.customer_id = customers.id
  ORDER BY customers.name
`

export const USAGE_EVENTS_SQL = `
  SELECT
    usage_events.id,
    usage_events.customer_id,
    customers.name AS customer_name,
    usage_events.units,
    usage_events.source,
    usage_events.idempotency_key,
    usage_events.created_at
  FROM usage_events
  INNER JOIN customers ON customers.id = usage_events.customer_id
  ORDER BY usage_events.id DESC
  LIMIT 24
`

export const BILLING_EVENTS_SQL = `
  SELECT
    id,
    provider_event_id,
    event_type,
    customer_external_id,
    version,
    outcome,
    processed_at
  FROM billing_events
  ORDER BY id DESC
  LIMIT 24
`

export const AUDIT_LOG_SQL = `
  SELECT id, actor, action, target, detail, created_at
  FROM audit_log
  ORDER BY id DESC
  LIMIT 24
`

export const INSERT_CUSTOMER_SQL = 'INSERT INTO customers (external_id, name, plan, status) VALUES (?, ?, ?, ?)'

export const INSERT_ENTITLEMENT_SQL = `
  INSERT INTO entitlements (customer_id, seats, api_quota, support_tier, active, version)
  SELECT id, ?, ?, ?, 1, ? FROM customers WHERE external_id = ?
`

export const INSERT_AUDIT_SQL = 'INSERT INTO audit_log (actor, action, target, detail) VALUES (?, ?, ?, ?)'

export const INSERT_USAGE_EVENT_SQL = `
  INSERT OR IGNORE INTO usage_events (customer_id, units, source, idempotency_key)
  VALUES (?, ?, ?, ?)
`

export const DECREMENT_USAGE_QUOTA_SQL = `
  UPDATE entitlements
  SET api_quota = max(api_quota - ?, 0),
      version = version + 1,
      updated_at = datetime('now')
  WHERE customer_id = ? AND changes() > 0
`

export const INSERT_USAGE_AUDIT_SQL = `
  INSERT INTO audit_log (actor, action, target, detail)
  SELECT ?, ?, ?, ? WHERE changes() > 0
`

export const INSERT_BILLING_EVENT_SQL = `
  INSERT OR IGNORE INTO billing_events (
    provider_event_id,
    event_type,
    customer_external_id,
    version,
    payload,
    outcome
  )
  VALUES (?, ?, ?, ?, ?, 'duplicate')
`

export const UPDATE_CUSTOMER_FROM_BILLING_SQL = `
  UPDATE customers
  SET plan = ?, status = ?
  WHERE external_id = ?
    AND id IN (SELECT customer_id FROM entitlements WHERE version < ?)
    AND EXISTS (
      SELECT 1 FROM billing_events
      WHERE provider_event_id = ? AND outcome = 'duplicate'
    )
`

export const UPDATE_ENTITLEMENT_FROM_BILLING_SQL = `
  UPDATE entitlements
  SET seats = ?,
      api_quota = ?,
      support_tier = ?,
      active = ?,
      version = ?,
      updated_at = datetime('now')
  WHERE customer_id = (SELECT id FROM customers WHERE external_id = ?)
    AND version < ?
    AND EXISTS (
      SELECT 1 FROM billing_events
      WHERE provider_event_id = ? AND outcome = 'duplicate'
    )
`

export const FINALIZE_BILLING_EVENT_SQL = `
  UPDATE billing_events
  SET outcome = CASE WHEN changes() > 0 THEN 'accepted' ELSE 'stale' END
  WHERE provider_event_id = ? AND outcome = 'duplicate'
`

export const INSERT_BILLING_AUDIT_SQL = `
  INSERT INTO audit_log (actor, action, target, detail)
  SELECT ?, ?, ?, ?
  WHERE changes() > 0
    AND EXISTS (
      SELECT 1 FROM billing_events
      WHERE provider_event_id = ? AND outcome = 'accepted'
    )
`

export const DELETE_BILLING_EVENTS_SQL = 'DELETE FROM billing_events'
export const DELETE_USAGE_EVENTS_SQL = 'DELETE FROM usage_events'
export const DELETE_AUDIT_LOG_SQL = 'DELETE FROM audit_log'
export const DELETE_ENTITLEMENTS_SQL = 'DELETE FROM entitlements'
export const DELETE_CUSTOMERS_SQL = 'DELETE FROM customers'
