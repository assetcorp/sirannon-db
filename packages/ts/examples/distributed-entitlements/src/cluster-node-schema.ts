export const REPLICATED_TABLES = ['customers', 'entitlements', 'usage_events', 'billing_events', 'audit_log'] as const

export const SCHEMA = `
  CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    plan TEXT NOT NULL CHECK (plan IN ('free', 'growth', 'scale', 'enterprise')),
    status TEXT NOT NULL CHECK (status IN ('active', 'past_due', 'suspended')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS entitlements (
    customer_id INTEGER PRIMARY KEY,
    seats INTEGER NOT NULL CHECK (seats >= 0),
    api_quota INTEGER NOT NULL CHECK (api_quota >= 0),
    support_tier TEXT NOT NULL CHECK (support_tier IN ('community', 'standard', 'priority', 'named')),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS usage_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    units INTEGER NOT NULL CHECK (units > 0),
    source TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS billing_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    customer_external_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    processed_at TEXT NOT NULL DEFAULT (datetime('now')),
    outcome TEXT NOT NULL CHECK (outcome IN ('accepted', 'duplicate', 'stale'))
  );

  CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
`

export const SEED_SQL = `
  INSERT OR IGNORE INTO customers (external_id, name, plan, status) VALUES
    ('cus_nova_forge', 'Nova Forge', 'scale', 'active'),
    ('cus_helios_labs', 'Helios Labs', 'growth', 'active'),
    ('cus_riverline_ai', 'Riverline AI', 'enterprise', 'active');

  INSERT OR IGNORE INTO entitlements (customer_id, seats, api_quota, support_tier, active, version)
  SELECT id, 48, 250000, 'priority', 1, 8 FROM customers WHERE external_id = 'cus_nova_forge';

  INSERT OR IGNORE INTO entitlements (customer_id, seats, api_quota, support_tier, active, version)
  SELECT id, 18, 75000, 'standard', 1, 4 FROM customers WHERE external_id = 'cus_helios_labs';

  INSERT OR IGNORE INTO entitlements (customer_id, seats, api_quota, support_tier, active, version)
  SELECT id, 120, 900000, 'named', 1, 12 FROM customers WHERE external_id = 'cus_riverline_ai';

  INSERT INTO audit_log (actor, action, target, detail)
  SELECT 'seed', 'seeded', 'control-plane', 'Loaded production-style entitlement records'
  WHERE NOT EXISTS (SELECT 1 FROM audit_log WHERE actor = 'seed' AND action = 'seeded');
`
