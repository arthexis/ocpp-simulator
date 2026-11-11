package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type remoteWireSummary struct {
	accepted   bool
	duplicate  bool
	stopReject bool
	resetReject bool
	authorized bool
	started    bool
	stopped    bool
	available  bool
	meters     int
}

func TestR2RemoteStartOCPPJLoopback(t *testing.T) {
	// All traffic stays inside an httptest WebSocket server. This is not a
	// connection to any external OCPP-CSMS or production charger.
	finished := make(chan remoteWireSummary, 1)
	finishHold := make(chan struct{})
	defer close(finishHold)
	failures := make(chan error, 1)
	report := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/R2-TEST" {
			http.NotFound(w, r)
			return
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"ocpp1.6"}}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			report(err)
			return
		}
		defer ws.Close()
		statuses := 0
		startSent := false
		var summary remoteWireSummary
		sendCall := func(id, action string, payload any) bool {
			if err := ws.WriteJSON([]any{2, id, action, payload}); err != nil {
				report(err)
				return false
			}
			return true
		}
		for {
			_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, bytes, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var frame []json.RawMessage
			if err := json.Unmarshal(bytes, &frame); err != nil || len(frame) < 3 {
				report(fmt.Errorf("invalid OCPP-J frame: %s", bytes))
				return
			}
			var kind int
			var id string
			if json.Unmarshal(frame[0], &kind) != nil || json.Unmarshal(frame[1], &id) != nil {
				report(fmt.Errorf("invalid message type/id: %s", bytes))
				return
			}
			switch kind {
			case 2: // Charge Point -> isolated local CSMS fixture
				if len(frame) != 4 {
					report(fmt.Errorf("bad CALL frame: %s", bytes))
					return
				}
				var action string
				_ = json.Unmarshal(frame[2], &action)
				var response any
				switch action {
				case "BootNotification":
					response = map[string]any{"status": "Accepted", "interval": 1, "currentTime": "2026-11-11T10:00:00Z"}
				case "StatusNotification":
					var payload struct {
						ConnectorID int    `json:"connectorId"`
						Status      string `json:"status"`
					}
					if err := json.Unmarshal(frame[3], &payload); err != nil {
						report(err)
						return
					}
					if payload.ConnectorID != 1 {
						report(fmt.Errorf("unexpected connector %d", payload.ConnectorID))
						return
					}
					if statuses == 0 {
						if payload.Status != "Available" {
							report(fmt.Errorf("expected initial Available, got %s", payload.Status))
							return
						}
						statuses++
					} else if payload.Status == "Available" {
						summary.available = true
					} else if payload.Status != "Preparing" && payload.Status != "Charging" {
						report(fmt.Errorf("unexpected OCPP status %s", payload.Status))
						return
					}
					response = map[string]any{}
				case "Authorize":
					summary.authorized = true
					var payload struct { IdTag string `json:"idTag"` }
					_ = json.Unmarshal(frame[3], &payload)
					if payload.IdTag != "R2-USER" {
						report(fmt.Errorf("wrong RFID %s", payload.IdTag))
						return
					}
					response = map[string]any{"idTagInfo": map[string]any{"status":"Accepted"}}
				case "StartTransaction":
					summary.started = true
					response = map[string]any{"idTagInfo": map[string]any{"status":"Accepted"}, "transactionId": 511}
				case "MeterValues":
					summary.meters++
					var payload map[string]any
					_ = json.Unmarshal(frame[3], &payload)
					if int(payload["transactionId"].(float64)) != 511 {
						report(fmt.Errorf("meter did not use CSMS transaction 511"))
						return
					}
					response = map[string]any{}
				case "StopTransaction":
					summary.stopped = true
					var payload map[string]any
					_ = json.Unmarshal(frame[3], &payload)
					if int(payload["transactionId"].(float64)) != 511 {
						report(fmt.Errorf("stop did not use CSMS transaction 511"))
						return
					}
					response = map[string]any{}
				case "Heartbeat":
					response = map[string]any{"currentTime":"2026-11-11T10:00:01Z"}
				default:
					report(fmt.Errorf("unexpected action %q", action))
					return
				}
				if err := ws.WriteJSON([]any{3, id, response}); err != nil {
					report(err)
					return
				}
				if action == "Heartbeat" && statuses == 1 && !startSent {
					// The first heartbeat proves Serve completed initial status
					// and enabled the dispatcher; no race with the startup gate.
					startSent = true
					if !sendCall("start-ok", "RemoteStartTransaction", map[string]any{"connectorId":1, "idTag":"R2-USER"}) {
						return
					}
				}
			case 3: // Charge Point -> CSMS response to a remote command
				var payload struct { Status string `json:"status"` }
				if err := json.Unmarshal(frame[2], &payload); err != nil {
					report(err)
					return
				}
				switch id {
				case "start-ok":
					if payload.Status != "Accepted" {
						report(fmt.Errorf("valid RemoteStart rejected: %s", payload.Status))
						return
					}
					summary.accepted = true
					if !sendCall("start-duplicate", "RemoteStartTransaction", map[string]any{"connectorId":1, "idTag":"R2-USER"}) {
						return
					}
				case "start-duplicate":
					if payload.Status != "Rejected" {
						report(fmt.Errorf("occupied connector accepted again: %s", payload.Status))
						return
					}
					summary.duplicate = true
					if !sendCall("stop-not-yet", "RemoteStopTransaction", map[string]any{"transactionId":511}) {
						return
					}
				case "stop-not-yet":
					if payload.Status != "Rejected" {
						report(fmt.Errorf("R3 stop unexpectedly enabled: %s", payload.Status))
						return
					}
					summary.stopReject = true
					if !sendCall("reset-not-yet", "Reset", map[string]any{"type":"Soft"}) {
						return
					}
				case "reset-not-yet":
					if payload.Status != "Rejected" {
						report(fmt.Errorf("R5 reset unexpectedly enabled: %s", payload.Status))
						return
					}
					summary.resetReject = true
				default:
					report(fmt.Errorf("unexpected remote command response %s", id))
					return
				}
			case 4:
				report(fmt.Errorf("unexpected OCPP CALLERROR: %s", bytes))
				return
			default:
				report(fmt.Errorf("invalid OCPP message type %d", kind))
				return
			}
			if summary.accepted && summary.duplicate && summary.stopReject && summary.resetReject &&
				summary.authorized && summary.started && summary.stopped && summary.available {
				finished <- summary
				<-finishHold
				return
			}
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- Serve(ctx, ServeOptions{
			Options: Options{
				CP: "R2-TEST",
				URL: "ws" + strings.TrimPrefix(server.URL, "http") + "/R2-TEST",
				Vendor: "Arthexis", Model: "Simulator",
			},
			ConnectTimeout: time.Second,
			Remote: true, Connectors: 1,
			RemotePowerKW: 7.2,
			RemoteDuration: 150*time.Millisecond,
			RemoteMeterInterval: 40*time.Millisecond,
		})
	}()
	var summary remoteWireSummary
	select {
	case summary = <-finished:
		if summary.meters == 0 {
			t.Fatal("RemoteStart produced no MeterValues")
		}
	case err := <-failures:
		t.Fatalf("local OCPP fixture: %v", err)
	case err := <-serveResult:
		t.Fatalf("Serve exited before remote transaction completed: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for actual RemoteStart/Stop transaction on loopback")
	}
	cancel()
	select {
	case err := <-serveResult:
		if err != nil && !strings.Contains(err.Error(), "disconnected") {
			t.Fatal(err)
		}
	case <-time.After(3*time.Second):
		t.Fatal("Serve did not exit after loopback test")
	}
	select {
	case err := <-failures:
		t.Fatalf("unexpected fixture failure: %v", err)
	default:
	}
}
