package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// This is an intentionally NEGATIVE end-to-end acceptance test: it proves why
// R0-R5 cannot be marked Ready. An in-process restart of Serve for the same
// CP identity announces Available even while the fake CSMS still owns an
// unconfirmed transaction from the previous Serve instance.
//
// It does not connect to a real CSMS or restart a system process. A durable
// journal plus reconciliation will have to change this assertion in a future
// separately approved recovery stage.
func TestR6ReproducesUnsafeFreshProcessAfterUnconfirmedTransaction(t *testing.T) {
	var connections atomic.Int32
	var csmsOpen atomic.Bool
	var stopObserved atomic.Int32
	firstUnconfirmed := make(chan struct{}, 1)
	newAvailable := make(chan bool, 1)
	fixtureErrors := make(chan error, 2)
	fail := func(err error) {
		select {
		case fixtureErrors <- err:
		default:
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/R6-CRASH-LOCAL" {
			http.NotFound(w, r)
			return
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"ocpp1.6"}}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			fail(err)
			return
		}
		defer ws.Close()
		n := connections.Add(1)
		initialAvailable := false
		remoteStartSent := false
		write := func(frame any) bool {
			if err := ws.WriteJSON(frame); err != nil {
				fail(err)
				return false
			}
			return true
		}
		for {
			_ = ws.SetReadDeadline(time.Now().Add(8 * time.Second))
			_, wire, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var frame []json.RawMessage
			if err := json.Unmarshal(wire, &frame); err != nil || len(frame) < 3 {
				fail(fmt.Errorf("invalid OCPP-J CALL: %s", wire))
				return
			}
			var kind int
			var id string
			if json.Unmarshal(frame[0], &kind) != nil || json.Unmarshal(frame[1], &id) != nil {
				fail(fmt.Errorf("invalid OCPP-J envelope: %s", wire))
				return
			}
			if kind == 3 {
				// RemoteStart Accepted acknowledges dispatch, not a CSMS tx close.
				continue
			}
			if kind != 2 || len(frame) != 4 {
				fail(fmt.Errorf("unexpected OCPP-J frame: %s", wire))
				return
			}
			var action string
			_ = json.Unmarshal(frame[2], &action)
			reply := any(map[string]any{})
			switch action {
			case "BootNotification":
				reply = map[string]any{
					"status": "Accepted", "interval": 1,
					"currentTime": "2026-11-11T12:00:00Z",
				}
			case "Heartbeat":
				reply = map[string]any{"currentTime": "2026-11-11T12:00:01Z"}
			case "StatusNotification":
				var status struct {
					Connector int `json:"connectorId"`
					State     string `json:"status"`
				}
				if json.Unmarshal(frame[3], &status) != nil || status.Connector != 1 {
					fail(fmt.Errorf("bad connector in StatusNotification: %s", frame[3]))
					return
				}
				if !initialAvailable {
					if status.State != "Available" {
						fail(fmt.Errorf("expected initial Available, got %s", status.State))
						return
					}
					initialAvailable = true
					if n == 2 {
						// Critical negative evidence: the old CSMS transaction
						// is still open but a fresh in-memory ledger permits
						// a new Available announcement for the SAME CP.
						select {
						case newAvailable <- csmsOpen.Load():
						default:
						}
					}
				}
			case "Authorize":
				reply = map[string]any{"idTagInfo": map[string]any{"status": "Accepted"}}
			case "StartTransaction":
				if n != 1 {
					fail(fmt.Errorf("unexpected StartTransaction on second Serve"))
					return
				}
				csmsOpen.Store(true)
				reply = map[string]any{
					"transactionId": 9001,
					"idTagInfo": map[string]any{"status": "Accepted"},
				}
			case "MeterValues":
				if n != 1 {
					fail(fmt.Errorf("unexpected MeterValues on second Serve"))
					return
				}
				var payload struct {
					TxID int `json:"transactionId"`
				}
				if json.Unmarshal(frame[3], &payload) != nil || payload.TxID != 9001 {
					fail(fmt.Errorf("meter used wrong CSMS transaction ID: %s", frame[3]))
					return
				}
				// Emulate a transport break while MeterValues is in flight.
				// The CSMS deliberately leaves the transaction open.
				_ = ws.Close()
				firstUnconfirmed <- struct{}{}
				return
			case "StopTransaction":
				stopObserved.Add(1)
				fail(fmt.Errorf("fixture unexpectedly received StopTransaction"))
				return
			default:
				fail(fmt.Errorf("unexpected OCPP action %q", action))
				return
			}
			if !write([]any{3, id, reply}) {
				return
			}
			if n == 1 && action == "Heartbeat" && initialAvailable && !remoteStartSent {
				remoteStartSent = true
				if !write([]any{2, "r6-start", "RemoteStartTransaction",
					map[string]any{"connectorId": 1, "idTag": "R6-LOCAL"}}) {
					return
				}
			}
		}
	}))
	defer server.Close()
	base := ServeOptions{
		Options: Options{
			CP: "R6-CRASH-LOCAL",
			URL: "ws" + strings.TrimPrefix(server.URL, "http") + "/R6-CRASH-LOCAL",
			Vendor: "Arthexis", Model: "Simulator",
		},
		ConnectTimeout: time.Second, Remote: true, Connectors: 1,
		RemotePowerKW: 7.2, RemoteDuration: 5 * time.Second,
		RemoteMeterInterval: 20 * time.Millisecond,
	}
	firstCtx, stopFirst := context.WithTimeout(context.Background(), 12*time.Second)
	defer stopFirst()
	firstResult := make(chan error, 1)
	go func() { firstResult <- Serve(firstCtx, base) }()
	select {
	case <-firstUnconfirmed:
	case err := <-fixtureErrors:
		t.Fatalf("first CSMS fixture failed: %v", err)
	case <-time.After(5*time.Second):
		t.Fatal("first session did not establish an unconfirmed transaction")
	}
	select {
	case err := <-firstResult:
		if err == nil {
			t.Fatal("unconfirmed first Serve shutdown incorrectly returned success")
		}
	case <-time.After(8*time.Second):
		t.Fatal("first Serve failed to terminate within its bounded shutdown")
	}
	if !csmsOpen.Load() || stopObserved.Load() != 0 {
		t.Fatal("CSMS fixture must retain the old transaction before restart")
	}

	// A distinct Serve call simulates a new process with a fresh in-memory
	// ledger. No production process is restarted and no state is persisted.
	secondCtx, stopSecond := context.WithTimeout(context.Background(), 8*time.Second)
	defer stopSecond()
	secondResult := make(chan error, 1)
	go func() { secondResult <- Serve(secondCtx, base) }()
	select {
	case stillOpen := <-newAvailable:
		if !stillOpen {
			t.Fatal("fixture lost its old CSMS transaction before the unsafe Available")
		}
		t.Log("NO-GO: fresh Go Serve announced Available while prior CSMS tx 9001 remained open")
	case err := <-fixtureErrors:
		t.Fatalf("second CSMS fixture failed: %v", err)
	case err := <-secondResult:
		t.Fatalf("second Serve exited before Available observation: %v", err)
	case <-time.After(3*time.Second):
		t.Fatal("second Serve did not reach initial Available")
	}
	stopSecond()
	select {
	case <-secondResult:
	case <-time.After(5*time.Second):
		t.Fatal("second Serve did not terminate after cancellation")
	}
	if connections.Load() != 2 || stopObserved.Load() != 0 || !csmsOpen.Load() {
		t.Fatalf("restart fixture invalid: sessions=%d stops=%d csmsOpen=%v",
			connections.Load(), stopObserved.Load(), csmsOpen.Load())
	}
}

func TestR6ManagerRestartLosesPendingAndKnownIDs(t *testing.T) {
	first, err := NewSessionManager(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.Reserve(1); err != nil {
		t.Fatal(err)
	}
	if err := first.BeginStart(1); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reserve(2); err != nil {
		t.Fatal(err)
	}
	if err := first.Start(2, 9202); err != nil {
		t.Fatal(err)
	}
	first.Close() // simulate a failed process with outstanding state
	previous := first.Unresolved()
	if len(previous) != 2 || previous[0].State != SessionStarting ||
		previous[0].TransactionID != 0 || previous[1].TransactionID != 9202 {
		t.Fatalf("first manager did not retain unresolved identities: %+v", previous)
	}
	second, err := NewSessionManager(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if len(second.Unresolved()) != 0 {
		t.Fatal("unexpected cross-instance state; test is not demonstrating volatility")
	}
	for connector := 1; connector <= 2; connector++ {
		snapshot, err := second.Snapshot(connector)
		if err != nil || snapshot.State != SessionAvailable {
			t.Fatalf("fresh manager state changed without a recovery protocol: %+v, %v", snapshot, err)
		}
	}
	t.Log("NO-GO: a new SessionManager has no record of CSMS tx 9202 or unknown pending Start")
}
