-- Offline disposable v8 schema fixture from arthexis/ocpp-csms
-- commit 77452e39efb1702cc78c753724d1d30892ae411d, CURRENT_SCHEMA_SQL.
-- Never point the offline adapter at an existing CSMS database.
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    charger_id TEXT NOT NULL,
    action TEXT NOT NULL,
    direction TEXT NOT NULL,
    transaction_id INTEGER,
    id_tag TEXT,
    charger_timestamp TEXT,
    payload_json TEXT NOT NULL
);
CREATE INDEX idx_events_charger_time ON events(charger_id, received_at);
CREATE INDEX idx_events_transaction ON events(transaction_id);
CREATE INDEX idx_events_card ON events(id_tag);
CREATE TABLE runtime_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    event TEXT NOT NULL,
    charger_id TEXT,
    details_json TEXT
);
CREATE INDEX idx_runtime_events_time ON runtime_events(occurred_at);
CREATE INDEX idx_runtime_events_charger_time ON runtime_events(charger_id, occurred_at);
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY,
    charger_id TEXT NOT NULL,
    connector_id INTEGER,
    id_tag TEXT,
    meter_start INTEGER,
    started_at TEXT,
    start_received_at TEXT,
    meter_stop INTEGER,
    stopped_at TEXT,
    stop_received_at TEXT,
    state TEXT NOT NULL,
    last_activity_at TEXT NOT NULL
);
CREATE INDEX idx_transactions_charger_state ON transactions(charger_id, state, start_received_at);
CREATE INDEX idx_transactions_connector_state ON transactions(charger_id, connector_id, state);
CREATE TABLE connector_status (
    info TEXT,
    charger_id TEXT NOT NULL,
    connector_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_code TEXT,
    event_timestamp TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY(charger_id, connector_id)
);
CREATE TABLE rfid_lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charger_id TEXT NOT NULL,
    list_version INTEGER NOT NULL,
    sent_at TEXT NOT NULL,
    source_file TEXT,
    list_hash TEXT NOT NULL,
    verified_version INTEGER
);
CREATE INDEX idx_rfid_lists_charger_sent ON rfid_lists(charger_id, sent_at);
CREATE INDEX idx_rfid_lists_charger_version ON rfid_lists(charger_id, list_version);
CREATE TABLE rfid_list_entries (
    list_id INTEGER NOT NULL,
    rfid TEXT NOT NULL,
    name TEXT,
    enabled INTEGER NOT NULL,
    PRIMARY KEY(list_id, rfid),
    FOREIGN KEY(list_id) REFERENCES rfid_lists(id) ON DELETE CASCADE
);
CREATE TABLE appliance_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE start_identities (
    charger_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    transaction_id INTEGER NOT NULL UNIQUE,
    PRIMARY KEY(charger_id, payload_json),
    FOREIGN KEY(transaction_id) REFERENCES transactions(transaction_id)
);
CREATE TABLE transaction_allocator (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    last_id INTEGER NOT NULL CHECK(last_id BETWEEN 0 AND 2147483647)
);
INSERT OR IGNORE INTO transaction_allocator(singleton,last_id)
SELECT 1, COALESCE(MAX(transaction_id),0) FROM transactions;
CREATE INDEX idx_transactions_boundary ON transactions(charger_id,connector_id,start_received_at,transaction_id);
CREATE VIEW transaction_boundaries AS
WITH ordered AS (
 SELECT transaction_id,
        LEAD(start_received_at) OVER (
          PARTITION BY charger_id,connector_id ORDER BY start_received_at,transaction_id
        ) AS next_start_received_at
 FROM transactions
)
SELECT t.transaction_id,t.charger_id,t.connector_id,
       t.start_received_at,t.stop_received_at AS observed_stop_received_at,
       o.next_start_received_at,t.stop_received_at AS observed_stop_at
FROM transactions AS t JOIN ordered AS o ON o.transaction_id=t.transaction_id;
CREATE VIEW transaction_summary AS
SELECT t.transaction_id,t.charger_id,t.connector_id,t.id_tag,
       t.started_at,t.stopped_at,t.state,t.meter_start,t.meter_stop,
       CASE WHEN t.meter_start IS NOT NULL AND t.meter_stop IS NOT NULL
                 AND t.meter_stop >= t.meter_start THEN t.meter_stop-t.meter_start END AS energy_wh,
       t.last_activity_at,t.start_received_at,t.stop_received_at,
       b.next_start_received_at,b.observed_stop_at
FROM transactions AS t JOIN transaction_boundaries AS b ON b.transaction_id=t.transaction_id;
-- Additive discovery session evidence in EventStore._ensure_session_evidence.
CREATE TABLE discovery_sessions (
    session_id TEXT PRIMARY KEY, charger_id TEXT NOT NULL,
    remote_ip TEXT NOT NULL, remote_port INTEGER NOT NULL,
    connected_at TEXT NOT NULL
);
CREATE TABLE discovery_event_sessions (
    event_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL,
    FOREIGN KEY(event_id) REFERENCES events(id)
);
CREATE INDEX idx_discovery_event_session ON discovery_event_sessions(session_id);
