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

// This is an in-process OCPP-J fixture, never a production CSMS connection.
// It verifies that the remote mode announces connector availability without
// generating Authorize, StartTransaction or MeterValues traffic.
func TestRemoteServeAnnouncesInitialAvailableOnWire(t *testing.T) {
	type statusRecord struct {
		connector int
		status    string
	}
	records := make(chan statusRecord, 2)
	heartbeats := make(chan struct{}, 1)
	serverErrors := make(chan error, 1)
	reportError := func(err error) {
		select {
		case serverErrors <- err:
		default:
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/SIM-R1" {
			http.NotFound(w, r)
			return
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"ocpp1.6"}}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			reportError(err)
			return
		}
		defer ws.Close()

		for {
			_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, packet, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var frame []json.RawMessage
			if err := json.Unmarshal(packet, &frame); err != nil || len(frame) != 4 {
				reportError(fmt.Errorf("invalid OCPP request frame: %s", packet))
				return
			}
			var messageType int
			var id, action string
			if err := json.Unmarshal(frame[0], &messageType); err != nil || messageType != 2 {
				reportError(fmt.Errorf("invalid OCPP message type: %s", frame[0]))
				return
			}
			if err := json.Unmarshal(frame[1], &id); err != nil {
				reportError(err)
				return
			}
			if err := json.Unmarshal(frame[2], &action); err != nil {
				reportError(err)
				return
			}
			var response any
			switch action {
			case "BootNotification":
				response = map[string]any{
					"currentTime": "2026-11-11T06:00:00Z",
					"interval":    1,
					"status":      "Accepted",
				}
			case "StatusNotification":
				var payload map[string]any
				if err := json.Unmarshal(frame[3], &payload); err != nil {
					reportError(err)
					return
				}
				connector, ok := payload["connectorId"].(float64)
				status, statusOK := payload["status"].(string)
				if !ok || !statusOK {
					reportError(fmt.Errorf("missing connector/status in %v", payload))
					return
				}
				records <- statusRecord{connector: int(connector), status: status}
				response = map[string]any{}
			case "Heartbeat":
				response = map[string]any{"currentTime": "2026-11-11T06:00:01Z"}
			default:
				reportError(fmt.Errorf("remote R1 emitted unexpected %s", action))
				return
			}
			if err := ws.WriteJSON([]any{3, id, response}); err != nil {
				reportError(err)
				return
			}
			if action == "Heartbeat" {
				select {
				case heartbeats <- struct{}{}:
				default:
				}
			}
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- Serve(ctx, ServeOptions{
			Options: Options{
				CP:     "SIM-R1",
				URL:    "ws" + strings.TrimPrefix(server.URL, "http") + "/SIM-R1",
				Vendor: "Arthexis",
				Model:  "Simulator",
			},
			ConnectTimeout: 2 * time.Second,
			Remote:         true,
			Connectors:     2,
		})
	}()

	for want := 1; want <= 2; want++ {
		select {
		case got := <-records:
			if got.connector != want || got.status != "Available" {
				t.Fatalf("initial OCPP status: got %+v, want connector %d Available", got, want)
			}
		case err := <-serverErrors:
			t.Fatalf("fixture error: %v", err)
		case err := <-result:
			t.Fatalf("Serve returned before initial statuses: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for initial connector Available")
		}
	}
	select {
	case <-heartbeats:
		// Serve reached its steady state after the initial status messages.
	case err := <-serverErrors:
		t.Fatalf("fixture error: %v", err)
	case err := <-result:
		t.Fatalf("Serve exited unexpectedly: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for first remote-mode Heartbeat")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("graceful remote-mode shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not shut down after cancellation")
	}
	select {
	case extra := <-records:
		t.Fatalf("duplicate/unexpected initial status: %+v", extra)
	default:
	}
	select {
	case err := <-serverErrors:
		t.Fatalf("unexpected fixture error: %v", err)
	default:
	}
}
