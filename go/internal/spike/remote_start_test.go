package spike

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
)

type remoteTestCP struct {
	mu         sync.Mutex
	events     []string
	authStatus types.AuthorizationStatus
	startStatus types.AuthorizationStatus
	authGate   <-chan struct{}
	startGate  <-chan struct{}
	startErr   error
	txID       int
}

func (cp *remoteTestCP) record(action string) {
	cp.mu.Lock()
	cp.events = append(cp.events, action)
	cp.mu.Unlock()
}
func (cp *remoteTestCP) history() string {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return strings.Join(cp.events, ",")
}
func (cp *remoteTestCP) StatusNotification(connector int, _ core.ChargePointErrorCode, status core.ChargePointStatus, _ ...func(*core.StatusNotificationRequest)) (*core.StatusNotificationConfirmation, error) {
	cp.record(string(status))
	return &core.StatusNotificationConfirmation{}, nil
}
func (cp *remoteTestCP) Authorize(_ string, _ ...func(*core.AuthorizeRequest)) (*core.AuthorizeConfirmation, error) {
	cp.record("Authorize")
	if cp.authGate != nil {
		<-cp.authGate
	}
	return &core.AuthorizeConfirmation{IdTagInfo: types.NewIdTagInfo(cp.authStatus)}, nil
}
func (cp *remoteTestCP) StartTransaction(_ int, _ string, _ int, _ *types.DateTime, _ ...func(*core.StartTransactionRequest)) (*core.StartTransactionConfirmation, error) {
	cp.record("StartTransaction")
	if cp.startGate != nil {
		<-cp.startGate
	}
	if cp.startErr != nil {
		return nil, cp.startErr
	}
	return &core.StartTransactionConfirmation{
		IdTagInfo: types.NewIdTagInfo(cp.startStatus), TransactionId: cp.txID,
	}, nil
}
func (cp *remoteTestCP) MeterValues(_ int, _ []types.MeterValue, _ ...func(*core.MeterValuesRequest)) (*core.MeterValuesConfirmation, error) {
	cp.record("MeterValues")
	return &core.MeterValuesConfirmation{}, nil
}
func (cp *remoteTestCP) StopTransaction(_ int, _ *types.DateTime, _ int, _ ...func(*core.StopTransactionRequest)) (*core.StopTransactionConfirmation, error) {
	cp.record("StopTransaction")
	return &core.StopTransactionConfirmation{}, nil
}
func remoteTestOptions() ChargeOptions {
	return ChargeOptions{
		Connector: 1, RFID: "TEST001", PowerKW: 7.2,
		Duration: 25 * time.Millisecond, MeterInterval: 10 * time.Millisecond,
	}
}
func remoteAcceptedFake() *remoteTestCP {
	return &remoteTestCP{
		authStatus: types.AuthorizationStatusAccepted,
		startStatus: types.AuthorizationStatusAccepted, txID: 91,
	}
}

func TestRemoteChargeTracksRealCSMSIDAndVerifiedFinish(t *testing.T) {
	cp := remoteAcceptedFake()
	m, err := NewSessionManager(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, err := m.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := RunRemoteCharge(ctx, cp, m, remoteTestOptions()); err != nil {
		t.Fatal(err)
	}
	requireSessionState(t, m, 1, SessionAvailable, 0)
	if _, ok := m.ledger.ConnectorFor(91); ok {
		t.Fatal("verified close retained CSMS ID")
	}
	for _, want := range []string{"Preparing,Authorize,StartTransaction,Charging,MeterValues", "StopTransaction,Available"} {
		if !strings.Contains(cp.history(), want) {
			t.Fatalf("missing %q in %s", want, cp.history())
		}
	}
}

func TestRemoteChargeRejectionAndAmbiguousStart(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*remoteTestCP)
		wantState SessionState
	}{
		{"authorization rejected", func(cp *remoteTestCP) { cp.authStatus = types.AuthorizationStatusInvalid }, SessionAvailable},
		{"start rejected", func(cp *remoteTestCP) { cp.startStatus = types.AuthorizationStatusInvalid }, SessionAvailable},
		{"unconfirmed start", func(cp *remoteTestCP) { cp.startErr = errors.New("lost response") }, SessionStarting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp := remoteAcceptedFake()
			tc.configure(cp)
			m, _ := NewSessionManager(context.Background(), 1)
			defer m.Close()
			ctx, err := m.Reserve(1)
			if err != nil {
				t.Fatal(err)
			}
			if err := RunRemoteCharge(ctx, cp, m, remoteTestOptions()); err == nil {
				t.Fatal("expected negative result")
			}
			requireSessionState(t, m, 1, tc.wantState, 0)
			if strings.Contains(cp.history(), "StopTransaction") {
				t.Fatal("fabricated StopTransaction on rejected or unknown start")
			}
			if tc.wantState == SessionStarting {
				if _, err := m.Reserve(1); err == nil {
					t.Fatal("reused connector with unconfirmed StartTransaction")
				}
			} else if _, err := m.Reserve(1); err != nil {
				t.Fatalf("explicit rejection did not free connector: %v", err)
			}
		})
	}
}

func TestRemoteStartDispatcherAdmissionAndLimits(t *testing.T) {
	gate := make(chan struct{})
	cp := remoteAcceptedFake()
	cp.authGate = gate
	m, _ := NewSessionManager(context.Background(), 2)
	d, err := NewRemoteStartDispatcher(m, cp, RemoteChargeSettings{
		PowerKW: 7.2, Duration: 100 * time.Millisecond, MeterInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	req := core.NewRemoteStartTransactionRequest("RFID001")
	if d.TryStart(req) {
		t.Fatal("accepted before initial Available")
	}
	if err := d.Enable(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []*core.RemoteStartTransactionRequest{
		nil, core.NewRemoteStartTransactionRequest(""),
		core.NewRemoteStartTransactionRequest(" leading"),
		core.NewRemoteStartTransactionRequest("TOO_LONG_RFID_1234567890"),
		{IdTag: "RFID001", ChargingProfile: &types.ChargingProfile{}},
		{IdTag: "RFID001", ConnectorId: func() *int { n := 3; return &n }()},
	} {
		if d.TryStart(invalid) {
			t.Fatalf("accepted invalid remote request: %+v", invalid)
		}
	}
	if !d.TryStart(req) {
		t.Fatal("first connector not admitted")
	}
	if !d.TryStart(req) {
		t.Fatal("second connector not admitted")
	}
	if d.TryStart(req) {
		t.Fatal("exceeded bounded connector count")
	}
	requireSessionState(t, m, 1, SessionReserved, 0)
	requireSessionState(t, m, 2, SessionReserved, 0)
	close(gate)
	d.Close()
	select {
	case <-d.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("bounded workers did not exit")
	}
	if d.TryStart(req) {
		t.Fatal("accepted after Close")
	}
}

func TestRemoteStartDispatcherConcurrentSameConnector(t *testing.T) {
	gate := make(chan struct{})
	cp := remoteAcceptedFake()
	cp.authGate = gate
	m, _ := NewSessionManager(context.Background(), 1)
	d, err := NewRemoteStartDispatcher(m, cp, RemoteChargeSettings{Duration: 100 * time.Millisecond, MeterInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Enable(); err != nil {
		t.Fatal(err)
	}
	connector := 1
	req := &core.RemoteStartTransactionRequest{IdTag: "TEST001", ConnectorId: &connector}
	var wg sync.WaitGroup
	results := make(chan bool, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- d.TryStart(req)
		}()
	}
	wg.Wait()
	close(results)
	count := 0
	for admitted := range results {
		if admitted {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected one accepted start, got %d", count)
	}
	close(gate)
	d.Close()
	select {
	case <-d.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("workers still active")
	}
}

func TestRemoteLateStartConfirmationAfterCancellation(t *testing.T) {
	gate := make(chan struct{})
	cp := remoteAcceptedFake()
	cp.startGate = gate
	m, _ := NewSessionManager(context.Background(), 1)
	ctx, err := m.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- RunRemoteCharge(ctx, cp, m, remoteTestOptions()) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot, _ := m.Snapshot(1)
		if snapshot.State == SessionStarting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never reached in-flight StartTransaction: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	m.Close()
	requireSessionState(t, m, 1, SessionStarting, 0)
	close(gate)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("late accepted start should be stopped and confirmed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("late StartTransaction did not terminate")
	}
	requireSessionState(t, m, 1, SessionAvailable, 0)
	if !strings.Contains(cp.history(), "StopTransaction,Available") {
		t.Fatal("late start never emitted actual StopTransaction")
	}
}

func TestRemoteSettingsValidation(t *testing.T) {
	for _, invalid := range []RemoteChargeSettings{
		{PowerKW: -1},
		{PowerKW: math.NaN()},
		{PowerKW: math.Inf(1)},
		{Duration: -time.Second},
		{Duration: maxRemoteDuration + time.Second},
		{Duration: time.Second, MeterInterval: 2 * time.Second},
	} {
		if _, err := invalid.normalized(); err == nil {
			t.Fatalf("accepted %+v", invalid)
		}
	}
	defaults, err := (RemoteChargeSettings{}).normalized()
	if err != nil || defaults.Duration != defaultRemoteDuration || defaults.PowerKW != 7.2 {
		t.Fatalf("bad defaults: %+v %v", defaults, err)
	}
}
