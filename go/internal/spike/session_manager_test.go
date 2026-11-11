package spike

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func requireSessionState(t *testing.T, m *SessionManager, connector int, want SessionState, txID int) {
	t.Helper()
	snapshot, err := m.Snapshot(connector)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != want || snapshot.TransactionID != txID {
		t.Fatalf("connector %d: got %+v; want %s / %d", connector, snapshot, want, txID)
	}
}

func TestSessionManagerLifecycleAndOwnership(t *testing.T) {
	m, err := NewSessionManager(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	requireSessionState(t, m, 1, SessionAvailable, 0)
	ctx, err := m.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	requireSessionState(t, m, 1, SessionReserved, 0)
	if _, err := m.Reserve(1); err == nil {
		t.Fatal("duplicate reservation")
	}
	if err := m.Start(1, 101); err != nil {
		t.Fatal(err)
	}
	if connector, ok := m.ledger.ConnectorFor(101); !ok || connector != 1 {
		t.Fatalf("transaction routing: %d %t", connector, ok)
	}
	if err := m.Finish(2, 101); err == nil {
		t.Fatal("wrong connector finished a transaction")
	}
	if err := m.RequestStop(1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("stop did not cancel session context")
	}
	requireSessionState(t, m, 1, SessionStopping, 101)
	if _, err := m.Reserve(1); err == nil {
		t.Fatal("reused stopping connector")
	}
	if err := m.RequestStop(1); err != nil {
		t.Fatal("repeated stop should remain idempotent")
	}
	if err := m.Finish(1, 102); err == nil {
		t.Fatal("accepted wrong transaction ID")
	}
	if err := m.Finish(1, 101); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.ledger.ConnectorFor(101); ok {
		t.Fatal("transaction mapping survived verified finish")
	}
	requireSessionState(t, m, 1, SessionAvailable, 0)
	if err := m.Finish(1, 101); err == nil {
		t.Fatal("accepted duplicate finish")
	}
}

func TestSessionManagerPendingCleanupAndLateConfirmation(t *testing.T) {
	m, err := NewSessionManager(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := m.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	active, err := m.Reserve(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(2, 202); err != nil {
		t.Fatal(err)
	}
	if err := m.Abort(2); err == nil {
		t.Fatal("abort must not erase active CSMS transaction")
	}
	m.Close()
	m.Close()
	select {
	case <-pending.Done():
	default:
		t.Fatal("pending session still active after Close")
	}
	select {
	case <-active.Done():
	default:
		t.Fatal("active session not canceled on Close")
	}
	requireSessionState(t, m, 1, SessionAvailable, 0)
	requireSessionState(t, m, 2, SessionStopping, 202)
	if connector, ok := m.ledger.ConnectorFor(202); !ok || connector != 2 {
		t.Fatal("Close fabricated transaction termination")
	}
	if _, err := m.Reserve(1); err == nil {
		t.Fatal("closed manager accepted reservation")
	}
	if err := m.Finish(2, 202); err != nil {
		t.Fatalf("late confirmed StopTransaction should clear mapping: %v", err)
	}
	requireSessionState(t, m, 2, SessionAvailable, 0)
}

func TestSessionManagerAbortAndFailureCleanup(t *testing.T) {
	m, err := NewSessionManager(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, err := m.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(1, 0); err == nil {
		t.Fatal("accepted invalid CSMS transaction ID")
	}
	requireSessionState(t, m, 1, SessionReserved, 0)
	if err := m.Abort(1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("Abort did not cancel reservation")
	}
	requireSessionState(t, m, 1, SessionAvailable, 0)
	if _, err := m.Reserve(1); err != nil {
		t.Fatal("abort did not release ledger reservation:", err)
	}
	if err := m.RequestStop(1); err != nil {
		t.Fatal(err)
	}
	requireSessionState(t, m, 1, SessionAvailable, 0)
}

func TestSessionManagerConcurrentReservationsAndBounds(t *testing.T) {
	for _, count := range []int{-1, 0, 129} {
		if _, err := NewSessionManager(context.Background(), count); err == nil {
			t.Fatalf("accepted %d connectors", count)
		}
	}
	m, err := NewSessionManager(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var wg sync.WaitGroup
	successes := make(chan context.Context, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ctx, err := m.Reserve(1); err == nil {
				successes <- ctx
			}
		}()
	}
	wg.Wait()
	close(successes)
	count := 0
	for range successes {
		count++
	}
	if count != 1 {
		t.Fatalf("expected one owner, got %d", count)
	}
	if _, err := m.Reserve(2); err == nil {
		t.Fatal("accepted out-of-range connector")
	}
	if _, err := m.Snapshot(0); err == nil {
		t.Fatal("accepted connector zero")
	}
}

func TestSessionManagerParentCancellationReleasesPending(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	m, err := NewSessionManager(parent, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := m.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("parent cancel did not cancel child context")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot, err := m.Snapshot(1)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.State == SessionAvailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("parent cancellation did not clean pending reservation: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := m.Reserve(1); err == nil {
		t.Fatal("accepted reservation after parent cancellation")
	}
}

func TestSessionManagerInitialAvailableOneShotAndFailures(t *testing.T) {
	m, err := NewSessionManager(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var got []int
	notify := func(connector int) error {
		got = append(got, connector)
		return nil
	}
	if err := m.AnnounceInitialAvailable(context.Background(), notify); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("wrong status order: %v", got)
	}
	if err := m.AnnounceInitialAvailable(context.Background(), notify); err == nil {
		t.Fatal("duplicate initial Available")
	}
	if len(got) != 3 {
		t.Fatal("duplicate notifier call")
	}

	m2, _ := NewSessionManager(context.Background(), 2)
	defer m2.Close()
	ctx, err := m2.Reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.AnnounceInitialAvailable(context.Background(), notify); err == nil {
		t.Fatal("advertised occupied connector as Available")
	}
	if err := m2.Abort(1); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	injected := errors.New("no status acknowledgement")
	err = m2.AnnounceInitialAvailable(context.Background(), func(connector int) error {
		if connector == 2 {
			return injected
		}
		return nil
	})
	if !errors.Is(err, injected) {
		t.Fatalf("missing status failure: %v", err)
	}
	if err := m2.AnnounceInitialAvailable(context.Background(), notify); err != nil {
		t.Fatalf("failed first notification must not mark initialization done: %v", err)
	}
}

func TestSessionManagerNoReservesDuringInitialAnnouncement(t *testing.T) {
	m, err := NewSessionManager(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	err = m.AnnounceInitialAvailable(context.Background(), func(int) error {
		if _, reserveErr := m.Reserve(1); reserveErr == nil {
			t.Fatal("reservation interleaved with initial Available")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reserve(1); err != nil {
		t.Fatal("reservation not restored after initial status:", err)
	}
}
