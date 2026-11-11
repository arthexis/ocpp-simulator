package spike

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// The schema fixture is copied from CSMS CURRENT_SCHEMA_SQL at
// arthexis/ocpp-csms@77452e39efb1702cc78c753724d1d30892ae411d.
// The fixture and all observations live exclusively in t.TempDir.
func csmsFixture(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "csms-v8.sqlite3")
	writer, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close() })
	schema, err := os.ReadFile(filepath.Join("testdata", "csms-v8-schema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(string(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec("PRAGMA user_version = 8"); err != nil {
		t.Fatal(err)
	}
	execCSMSFixture(t, writer, "INSERT INTO appliance_metadata(key,value) VALUES('source_id','source-A')")
	// mode=ro prevents a consumer from writing the disposable fixture.
	reader, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	reader.SetMaxOpenConns(1)
	t.Cleanup(func() { reader.Close() })
	return writer, reader
}

func execCSMSFixture(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("fixture SQL: %v: %s", err, q)
	}
}

const fixtureTime = "2026-11-11T10:00:00+00:00"

func fixtureStart(t *testing.T, db *sql.DB, cp string, connector int, id int64, payload string) {
	t.Helper()
	execCSMSFixture(t, db, "INSERT INTO transactions(transaction_id,charger_id,connector_id,state,start_received_at,last_activity_at) VALUES(?,?,?,'open',?,?)", id, cp, connector, fixtureTime, fixtureTime)
	execCSMSFixture(t, db, "INSERT INTO events(received_at,charger_id,action,direction,transaction_id,payload_json) VALUES(?,?,'StartTransaction','in',?,?)", fixtureTime, cp, id, payload)
}
func fixtureStop(t *testing.T, db *sql.DB, cp string, id int64) {
	t.Helper()
	payload := fmt.Sprintf("{\"transaction_id\":%d,\"meter_stop\":42,\"timestamp\":\"2026-11-11T10:00:00Z\"}", id)
	execCSMSFixture(t, db, "INSERT INTO events(received_at,charger_id,action,direction,transaction_id,payload_json) VALUES(?,?,'StopTransaction','in',?,?)", fixtureTime, cp, id, payload)
	execCSMSFixture(t, db, "UPDATE transactions SET state='stopped',stop_received_at=?,meter_stop=42,stopped_at='2026-11-11T10:00:00Z' WHERE transaction_id=? AND charger_id=?", fixtureTime, id, cp)
}

func assertAlwaysQuarantined(t *testing.T, obs csmsV8Observation) {
	t.Helper()
	if obs.Trust != "Quarantined" {
		t.Fatalf("observation granted authority: %+v", obs)
	}
}

func TestR6C3AExhaustiveCSMSV8InventoryAndRealStopReceipt(t *testing.T) {
	writer, reader := csmsFixture(t)
	payload := "{\"connector_id\":1,\"id_tag\":\"RFID\",\"meter_start\":0,\"timestamp\":\"2026-11-11T10:00:00Z\"}"
	fixtureStart(t, writer, "CP-A", 1, 101, payload)
	fixtureStart(t, writer, "CP-A", 2, 102, strings.Replace(payload, "\"connector_id\":1", "\"connector_id\":2", 1))
	fixtureStop(t, writer, "CP-A", 102)
	fixtureStart(t, writer, "CP-B", 1, 103, payload)
	execCSMSFixture(t, writer, "INSERT INTO connector_status(charger_id,connector_id,status,event_timestamp,received_at) VALUES('CP-A',2,'Available',?,?)", fixtureTime, fixtureTime)
	obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 3)
	if err != nil {
		t.Fatal(err)
	}
	assertAlwaysQuarantined(t, obs)
	if obs.SchemaVersion != 8 || obs.SourceID != "source-A" || obs.EventWatermark != 4 || len(obs.Connectors) != 3 {
		t.Fatalf("schema/source/watermark/coverage: %+v", obs)
	}
	if len(obs.Conflicts) != 0 || len(obs.OrphanStopEventIDs) != 0 {
		t.Fatalf("unexpected conflicts: %+v", obs)
	}
	if len(obs.Connectors[0].Transactions) != 1 || obs.Connectors[0].Transactions[0].State != "open" {
		t.Fatalf("missing open transaction: %+v", obs.Connectors[0])
	}
	if len(obs.Connectors[1].Transactions) != 1 || obs.Connectors[1].Transactions[0].StopReceipt == nil {
		t.Fatalf("no authoritative matching StopTransaction receipt: %+v", obs.Connectors[1])
	}
	if obs.Connectors[1].Transactions[0].StopReceipt.ReceivedAt != fixtureTime || obs.Connectors[1].Status != "Available" {
		t.Fatalf("stop/status mismatch: %+v", obs.Connectors[1])
	}
	if len(obs.Connectors[2].Transactions) != 0 {
		t.Fatalf("empty connector not accounted for: %+v", obs.Connectors[2])
	}
}

func TestR6C3ANegativeEvidenceNeverGrantsCleanVerified(t *testing.T) {
	t.Run("empty inventory and zero watermark", func(t *testing.T) {
		_, reader := csmsFixture(t)
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 2)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if len(obs.Connectors) != 2 || obs.EventWatermark != 0 { t.Fatalf("empty coverage: %+v", obs) }
	})
	t.Run("Available contradicts persisted open", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		fixtureStart(t, writer, "CP-A", 1, 101, "{\"connector_id\":1}")
		execCSMSFixture(t, writer, "INSERT INTO connector_status(charger_id,connector_id,status,event_timestamp,received_at) VALUES('CP-A',1,'Available',?,?)", fixtureTime, fixtureTime)
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if len(obs.Conflicts) == 0 { t.Fatal("ignored Available/open conflict") }
	})
	t.Run("identical payload retry has no attempt binding", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		fixtureStart(t, writer, "CP-A", 1, 101, "{\"connector_id\":1}")
		execCSMSFixture(t, writer, "INSERT INTO events(received_at,charger_id,action,direction,transaction_id,payload_json) VALUES(?,?,'StartTransaction','in',101,'{\"connector_id\":1}')", fixtureTime, "CP-A")
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if obs.Connectors[0].Transactions[0].DuplicateStarts != 1 { t.Fatalf("duplicate not reported: %+v", obs) }
	})
	t.Run("unknown and cross CP orphans do not close", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		fixtureStart(t, writer, "CP-B", 1, 202, "{\"connector_id\":1}")
		execCSMSFixture(t, writer, "INSERT INTO events(received_at,charger_id,action,direction,transaction_id,payload_json) VALUES(?,?,'StopTransaction','in',202,'{\"transaction_id\":202}')", fixtureTime, "CP-A")
		execCSMSFixture(t, writer, "INSERT INTO events(received_at,charger_id,action,direction,transaction_id,payload_json) VALUES(?,?,'StopTransaction','in',999,'{\"transaction_id\":999}')", fixtureTime, "CP-A")
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if len(obs.OrphanStopEventIDs) != 2 || len(obs.Connectors[0].Transactions) != 0 { t.Fatalf("orphan misclassified: %+v", obs) }
	})
	t.Run("missing real StopTransaction receipt is not next-start boundary", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		fixtureStart(t, writer, "CP-A", 1, 101, "{\"connector_id\":1}")
		execCSMSFixture(t, writer, "UPDATE transactions SET state='stopped',stop_received_at=? WHERE transaction_id=101", fixtureTime)
		fixtureStart(t, writer, "CP-A", 1, 102, "{\"connector_id\":1,\"timestamp\":\"later\"}")
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if len(obs.Conflicts) == 0 || obs.Connectors[0].Transactions[0].StopReceipt != nil { t.Fatalf("inferred stop accepted: %+v", obs) }
	})
	t.Run("duplicate stop receipt preserves first", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		fixtureStart(t, writer, "CP-A", 1, 101, "{\"connector_id\":1}")
		fixtureStop(t, writer, "CP-A", 101)
		execCSMSFixture(t, writer, "INSERT INTO events(received_at,charger_id,action,direction,transaction_id,payload_json) VALUES(?,?,'StopTransaction','in',101,'{\"transaction_id\":101}')", fixtureTime, "CP-A")
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		r := obs.Connectors[0].Transactions[0]
		if r.StopReceipt == nil || r.DuplicateStops != 1 { t.Fatalf("duplicate destroyed first receipt: %+v", r) }
	})
	t.Run("open row with matching stop event timestamp is not a receipt", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		fixtureStart(t, writer, "CP-A", 1, 101, "{\"connector_id\":1}")
		fixtureStop(t, writer, "CP-A", 101)
		execCSMSFixture(t, writer, "UPDATE transactions SET state='open' WHERE transaction_id=101")
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if obs.Connectors[0].Transactions[0].StopReceipt != nil || len(obs.Conflicts) == 0 {
			t.Fatalf("open row falsely certified by matching stop event: %+v", obs)
		}
	})
	t.Run("same stop timestamp but wrong meter is not a receipt", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		fixtureStart(t, writer, "CP-A", 1, 101, "{\"connector_id\":1}")
		fixtureStop(t, writer, "CP-A", 101)
		execCSMSFixture(t, writer, "UPDATE transactions SET meter_stop=43 WHERE transaction_id=101")
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if obs.Connectors[0].Transactions[0].StopReceipt != nil || len(obs.Conflicts) == 0 {
			t.Fatalf("mismatched stop payload falsely certified: %+v", obs)
		}
	})
	t.Run("overlapping opens conflict", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		fixtureStart(t, writer, "CP-A", 1, 101, "{\"connector_id\":1}")
		fixtureStart(t, writer, "CP-A", 1, 102, "{\"connector_id\":1,\"timestamp\":\"later\"}")
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if len(obs.Conflicts) == 0 { t.Fatal("overlapping open transactions not detected") }
	})
	t.Run("WebSocket replacement is not fenced", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		execCSMSFixture(t, writer, "INSERT INTO discovery_sessions(session_id,charger_id,remote_ip,remote_port,connected_at) VALUES('old','CP-A','127.0.0.1',1,?)", fixtureTime)
		execCSMSFixture(t, writer, "INSERT INTO discovery_sessions(session_id,charger_id,remote_ip,remote_port,connected_at) VALUES('new','CP-A','127.0.0.1',2,?)", fixtureTime)
		obs, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, obs)
		if len(obs.Connectors) != 1 { t.Fatal("lost connector coverage on socket change") }
	})
	t.Run("watermark advances but is not an owner epoch", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		first, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		execCSMSFixture(t, writer, "INSERT INTO events(received_at,charger_id,action,direction,payload_json) VALUES(?,?,'Heartbeat','in','{}')", fixtureTime, "CP-A")
		second, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1)
		if err != nil { t.Fatal(err) }
		assertAlwaysQuarantined(t, first)
		assertAlwaysQuarantined(t, second)
		if second.EventWatermark <= first.EventWatermark { t.Fatalf("watermark failed to advance: %d->%d",first.EventWatermark,second.EventWatermark) }
	})
	t.Run("source mismatch and unsupported schema fail closed", func(t *testing.T) {
		writer, reader := csmsFixture(t)
		if _, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-B", 1); err == nil { t.Fatal("foreign source accepted") }
		execCSMSFixture(t, writer, "PRAGMA user_version = 7")
		if _, err := observeCSMSV8Fixture(context.Background(), reader, "CP-A", "source-A", 1); err == nil { t.Fatal("non-v8 schema accepted") }
	})
}
