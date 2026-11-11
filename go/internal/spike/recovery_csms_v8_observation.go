package spike

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// R6C-3A is an offline evidence reader, deliberately unreachable from Serve.
// Callers supply a disposable, read-only SQLite v8 handle. It never opens a
// network connection, changes CSMS state or grants recovery authority.
type csmsV8Receipt struct {
	EventID    int64
	ReceivedAt string
}

type csmsV8Transaction struct {
	ID              int64
	Connector       int
	State           string
	StopReceivedAt  string
	StopReceipt     *csmsV8Receipt
	DuplicateStarts int
	DuplicateStops  int
}

type csmsV8Connector struct {
	ID           int
	Status       string
	Transactions []*csmsV8Transaction
	Conflicts    []string
}

type csmsV8Observation struct {
	SourceID          string
	CPID              string
	SchemaVersion     int
	EventWatermark    int64 // Observational events.id, NEVER an owner epoch/revision.
	Connectors        []csmsV8Connector
	OrphanStopEventIDs []int64
	Conflicts         []string
	Trust             string // Unconditionally Quarantined.
}

// observeCSMSV8Fixture never resolves an attempt_id, claims ownership, or
// correlates WebSocket sessions to Go journal attempts. Every query shares one
// SQLite read transaction. No absence of evidence can turn into permission.
func observeCSMSV8Fixture(ctx context.Context, db *sql.DB, cpID, expectedSource string, count int) (csmsV8Observation, error) {
	out := csmsV8Observation{CPID: cpID, Trust: "Quarantined"}
	if db == nil || strings.TrimSpace(cpID) == "" || strings.TrimSpace(expectedSource) == "" || count < 1 || count > 128 {
		return out, fmt.Errorf("invalid offline CSMS observation identity")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, fmt.Errorf("begin read-only CSMS snapshot: %w", err)
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&out.SchemaVersion); err != nil || out.SchemaVersion != 8 {
		return out, fmt.Errorf("expected CSMS schema v8, got %d: %v", out.SchemaVersion, err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT value FROM appliance_metadata WHERE key = 'source_id'").Scan(&out.SourceID); err != nil || out.SourceID != expectedSource {
		return out, fmt.Errorf("CSMS source_id absent or mismatched: %v", err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0) FROM events").Scan(&out.EventWatermark); err != nil {
		return out, fmt.Errorf("CSMS event watermark: %w", err)
	}
	out.Connectors = make([]csmsV8Connector, count)
	for i := range out.Connectors {
		out.Connectors[i].ID = i + 1
	}
	statuses, err := tx.QueryContext(ctx, "SELECT connector_id, status FROM connector_status WHERE charger_id = ? ORDER BY connector_id", cpID)
	if err != nil {
		return out, fmt.Errorf("read CSMS statuses: %w", err)
	}
	for statuses.Next() {
		var connector int
		var status string
		if err := statuses.Scan(&connector, &status); err != nil {
			statuses.Close()
			return out, err
		}
		if connector == 0 { // CP-level OCPP status is not a physical connector.
			continue
		}
		if connector < 1 || connector > count {
			out.Conflicts = append(out.Conflicts, fmt.Sprintf("out-of-range status connector %d", connector))
		} else {
			out.Connectors[connector-1].Status = status
		}
	}
	if err := statuses.Err(); err != nil {
		statuses.Close()
		return out, err
	}
	statuses.Close()

	byID := make(map[int64]*csmsV8Transaction)
	rows, err := tx.QueryContext(ctx, "SELECT transaction_id, connector_id, state, stop_received_at FROM transactions WHERE charger_id = ? ORDER BY transaction_id", cpID)
	if err != nil {
		return out, fmt.Errorf("read CSMS transactions: %w", err)
	}
	for rows.Next() {
		var id int64
		var connector sql.NullInt64
		var state string
		var stop sql.NullString
		if err := rows.Scan(&id, &connector, &state, &stop); err != nil {
			rows.Close()
			return out, err
		}
		if id <= 0 || !connector.Valid || connector.Int64 < 1 || connector.Int64 > int64(count) {
			out.Conflicts = append(out.Conflicts, fmt.Sprintf("unassigned/out-of-range CSMS transaction %d", id))
			continue
		}
		record := &csmsV8Transaction{ID: id, Connector: int(connector.Int64), State: state}
		if stop.Valid {
			record.StopReceivedAt = stop.String
		}
		out.Connectors[record.Connector-1].Transactions = append(out.Connectors[record.Connector-1].Transactions, record)
		byID[id] = record
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()

	// Source events are the actual receipt evidence. Derived
	// transaction_boundaries and a reported Available are NEVER receipts.
	events, err := tx.QueryContext(ctx, "SELECT id, received_at, action, transaction_id, payload_json FROM events WHERE charger_id = ? AND direction = 'in' AND action IN ('StartTransaction', 'StopTransaction') ORDER BY id", cpID)
	if err != nil {
		return out, fmt.Errorf("read CSMS receipts: %w", err)
	}
	payloadOwners := make(map[string]int64)
	stopCounts := make(map[int64]int)
	for events.Next() {
		var eventID int64
		var receivedAt, action, payloadJSON string
		var transactionID sql.NullInt64
		if err := events.Scan(&eventID, &receivedAt, &action, &transactionID, &payloadJSON); err != nil {
			events.Close()
			return out, err
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			events.Close()
			return out, fmt.Errorf("invalid CSMS event %d JSON: %w", eventID, err)
		}
		var record *csmsV8Transaction
		if transactionID.Valid {
			record = byID[transactionID.Int64]
		}
		if action == "StopTransaction" {
			wireID, valid := payload["transaction_id"].(float64)
			if !valid || !transactionID.Valid || wireID != float64(transactionID.Int64) || record == nil {
				out.OrphanStopEventIDs = append(out.OrphanStopEventIDs, eventID)
				continue
			}
			stopCounts[record.ID]++
			if record.StopReceivedAt != "" && receivedAt == record.StopReceivedAt && record.StopReceipt == nil {
				record.StopReceipt = &csmsV8Receipt{EventID: eventID, ReceivedAt: receivedAt}
			}
			continue
		}
		if record == nil {
			out.Conflicts = append(out.Conflicts, fmt.Sprintf("unmatched StartTransaction event %d", eventID))
			continue
		}
		// Canonicalize as in CSMS commit_start (sorted keys, compact JSON).
		// Repetition can be a retry OR a legitimate second session; no
		// attempt_id or OCPP-J UniqueId is stored here.
		canonical, err := json.Marshal(payload)
		if err != nil {
			events.Close()
			return out, err
		}
		key := string(canonical)
		if owner, found := payloadOwners[key]; found {
			if owner != record.ID {
				out.Conflicts = append(out.Conflicts, fmt.Sprintf("same Start payload maps to transactions %d and %d", owner, record.ID))
			} else {
				record.DuplicateStarts++
			}
		} else {
			payloadOwners[key] = record.ID
		}
	}
	if err := events.Err(); err != nil {
		events.Close()
		return out, err
	}
	events.Close()

	for i := range out.Connectors {
		c := &out.Connectors[i]
		openCount := 0
		for _, record := range c.Transactions {
			if n := stopCounts[record.ID]; n > 1 {
				record.DuplicateStops = n - 1
			}
			switch {
			case record.State == "open" && record.StopReceivedAt == "":
				openCount++
				if stopCounts[record.ID] > 0 {
					c.Conflicts = append(c.Conflicts, fmt.Sprintf("open transaction %d with stop events", record.ID))
				}
				if c.Status == "Available" {
					c.Conflicts = append(c.Conflicts, fmt.Sprintf("Available conflicts with open transaction %d", record.ID))
				}
			case record.State == "stopped" && record.StopReceivedAt != "" && record.StopReceipt != nil:
				// Matched real stop receipt: observed closure in the CSMS
				// ledger, not proof of owner fencing or physical idleness.
			default:
				c.Conflicts = append(c.Conflicts, fmt.Sprintf("transaction %d has inconsistent state/stop receipt", record.ID))
			}
		}
		if openCount > 1 {
			c.Conflicts = append(c.Conflicts, fmt.Sprintf("%d overlapping open transactions", openCount))
		}
		out.Conflicts = append(out.Conflicts, c.Conflicts...)
	}
	// The transaction's read-consistency does not persist a fence across
	// WebSocket replacement or future CSMS writes. No CleanVerified route.
	return out, nil
}
