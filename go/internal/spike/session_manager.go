package spike

import (
	"context"
	"fmt"
	"sync"
)

// SessionState describes local connector ownership, not an OCPP confirmation.
// Stopping retains the CSMS transaction ID until a verified Finish.
type SessionState string

const (
	SessionAvailable SessionState = "Available"
	SessionReserved  SessionState = "Reserved"
	SessionStarting  SessionState = "Starting"
	SessionCharging  SessionState = "Charging"
	SessionStopping  SessionState = "Stopping"
)

// SessionSnapshot is an immutable copy of local session bookkeeping.
type SessionSnapshot struct {
	Connector     int
	State         SessionState
	TransactionID int
}

type managedSession struct {
	state         SessionState
	transactionID int
	ctx           context.Context
	cancel        context.CancelFunc
}

// SessionManager bounds ownership and transaction identity per connector.
// A pending StartTransaction stays reserved until its response is known.
type SessionManager struct {
	mu         sync.Mutex
	parent     context.Context
	ledger     *ConnectorLedger
	slots      []managedSession // connector IDs start at 1; slot 0 is unused
	closed     bool
	announcing bool
	announced  bool
	stopParent func() bool
}

func NewSessionManager(parent context.Context, connectors int) (*SessionManager, error) {
	if parent == nil || parent.Err() != nil {
		return nil, fmt.Errorf("session parent context must be active")
	}
	ledger, err := NewConnectorLedger(connectors)
	if err != nil {
		return nil, err
	}
	m := &SessionManager{
		parent: parent,
		ledger: ledger,
		slots:  make([]managedSession, connectors+1),
	}
	for connector := 1; connector <= connectors; connector++ {
		m.slots[connector].state = SessionAvailable
	}
	m.mu.Lock()
	m.stopParent = context.AfterFunc(parent, m.Close)
	m.mu.Unlock()
	return m, nil
}

func (m *SessionManager) validConnector(connector int) bool {
	return connector > 0 && connector < len(m.slots)
}

func (m *SessionManager) Snapshot(connector int) (SessionSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.validConnector(connector) {
		return SessionSnapshot{}, fmt.Errorf("invalid connector %d", connector)
	}
	slot := m.slots[connector]
	return SessionSnapshot{Connector: connector, State: slot.state, TransactionID: slot.transactionID}, nil
}

// Reserve atomically obtains the ledger reservation and a cancelable context.
func (m *SessionManager) Reserve(connector int) (context.Context, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.parent.Err() != nil || m.announcing {
		return nil, fmt.Errorf("session manager not accepting reservations")
	}
	if !m.validConnector(connector) {
		return nil, fmt.Errorf("invalid connector %d", connector)
	}
	if m.slots[connector].state != SessionAvailable {
		return nil, fmt.Errorf("connector %d is %s", connector, m.slots[connector].state)
	}
	if err := m.ledger.Reserve(connector); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(m.parent)
	m.slots[connector] = managedSession{state: SessionReserved, ctx: ctx, cancel: cancel}
	return ctx, nil
}

// ReserveAny selects the lowest available connector under the same lock as
// the ledger reservation. Competing remote requests cannot choose the same slot.
func (m *SessionManager) ReserveAny() (int, context.Context, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.parent.Err() != nil || m.announcing {
		return 0, nil, fmt.Errorf("session manager not accepting reservations")
	}
	for connector := 1; connector < len(m.slots); connector++ {
		if m.slots[connector].state != SessionAvailable {
			continue
		}
		if err := m.ledger.Reserve(connector); err != nil {
			return 0, nil, err
		}
		ctx, cancel := context.WithCancel(m.parent)
		m.slots[connector] = managedSession{state: SessionReserved, ctx: ctx, cancel: cancel}
		return connector, ctx, nil
	}
	return 0, nil, fmt.Errorf("all connectors occupied")
}

// BeginStart marks the window when the CSMS may allocate a real transaction.
// Until its response arrives, cancellation must not drop this reservation.
func (m *SessionManager) BeginStart(connector int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.parent.Err() != nil || !m.validConnector(connector) {
		return fmt.Errorf("session manager closed or invalid connector")
	}
	slot := &m.slots[connector]
	if slot.state != SessionReserved || slot.ctx.Err() != nil {
		return fmt.Errorf("connector %d not ready to start", connector)
	}
	slot.state = SessionStarting
	return nil
}

// FailStart releases a pending StartTransaction only after the CSMS has
// explicitly rejected it or its request has failed without an accepted ID.
// A transport timeout can be ambiguous and must be treated by the caller as
// an unknown transaction rather than a successful closure.
func (m *SessionManager) FailStart(connector int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.validConnector(connector) || m.slots[connector].state != SessionStarting {
		return fmt.Errorf("connector %d has no pending start", connector)
	}
	m.slots[connector].cancel()
	m.ledger.Abort(connector)
	m.slots[connector] = managedSession{state: SessionAvailable}
	return nil
}

// Start records a CSMS-issued transaction ID; it never fabricates one.
func (m *SessionManager) Start(connector, transactionID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.validConnector(connector) {
		return fmt.Errorf("invalid connector %d", connector)
	}
	slot := &m.slots[connector]
	if slot.state != SessionReserved && slot.state != SessionStarting {
		return fmt.Errorf("connector %d has no pending reservation", connector)
	}
	// Preserve backwards compatibility with direct Reserve→Start callers, but
	// allow an in-flight BeginStart to record late CSMS acknowledgement even
	// if shutdown canceled its context.
	if slot.state == SessionReserved && (m.closed || m.parent.Err() != nil || slot.ctx.Err() != nil) {
		return fmt.Errorf("connector %d reservation canceled", connector)
	}
	if err := m.ledger.Start(connector, transactionID); err != nil {
		return err
	}
	if m.closed || m.parent.Err() != nil || slot.ctx.Err() != nil {
		slot.state = SessionStopping
		slot.cancel()
	} else {
		slot.state = SessionCharging
	}
	slot.transactionID = transactionID
	return nil
}

// Abort releases only a reservation that has not started a CSMS transaction.
func (m *SessionManager) Abort(connector int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.validConnector(connector) || m.slots[connector].state != SessionReserved {
		return fmt.Errorf("connector %d has no abortable reservation", connector)
	}
	m.slots[connector].cancel()
	m.ledger.Abort(connector)
	m.slots[connector] = managedSession{state: SessionAvailable}
	return nil
}

// RequestStop cancels a session context. An active transaction stays in the
// ledger until Finish receives evidence of an actual StopTransaction success.
func (m *SessionManager) RequestStop(connector int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.validConnector(connector) {
		return fmt.Errorf("invalid connector %d", connector)
	}
	slot := &m.slots[connector]
	switch slot.state {
	case SessionReserved:
		slot.cancel()
		m.ledger.Abort(connector)
		*slot = managedSession{state: SessionAvailable}
	case SessionStarting:
		slot.cancel() // keep reservation until StartTransaction result
	case SessionCharging:
		slot.state = SessionStopping
		slot.cancel()
	case SessionStopping:
		slot.cancel() // idempotent; do not release the transaction ID
	default:
		return fmt.Errorf("connector %d has no active session", connector)
	}
	return nil
}

// Finish must be called only after confirmed transaction closure. It remains
// possible after Close so a late verified StopTransaction can clear the ledger.
func (m *SessionManager) Finish(connector, transactionID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.validConnector(connector) ||
		(m.slots[connector].state != SessionCharging && m.slots[connector].state != SessionStopping) {
		return fmt.Errorf("connector %d has no started transaction", connector)
	}
	if err := m.ledger.Finish(connector, transactionID); err != nil {
		return err
	}
	m.slots[connector].cancel()
	m.slots[connector] = managedSession{state: SessionAvailable}
	return nil
}

// AnnounceInitialAvailable sends each connector's initial status exactly once.
// A failed notification is propagated; it does not mark initialization done.
// Reservations cannot race with these initial announcements.
func (m *SessionManager) AnnounceInitialAvailable(ctx context.Context, notify func(int) error) error {
	if ctx == nil || notify == nil {
		return fmt.Errorf("initial status requires context and notifier")
	}
	m.mu.Lock()
	if m.closed || m.parent.Err() != nil || ctx.Err() != nil || m.announcing || m.announced {
		m.mu.Unlock()
		return fmt.Errorf("initial connector status unavailable")
	}
	for connector := 1; connector < len(m.slots); connector++ {
		if m.slots[connector].state != SessionAvailable {
			m.mu.Unlock()
			return fmt.Errorf("connector %d not available for initial status", connector)
		}
	}
	m.announcing = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.announcing = false
		m.mu.Unlock()
	}()
	for connector := 1; connector < len(m.slots); connector++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.parent.Err(); err != nil {
			return err
		}
		m.mu.Lock()
		closed := m.closed
		m.mu.Unlock()
		if closed {
			return fmt.Errorf("session manager closed during initial status")
		}
		if err := notify(connector); err != nil {
			return fmt.Errorf("connector %d initial Available: %w", connector, err)
		}
	}
	m.mu.Lock()
	if !m.closed && m.parent.Err() == nil && ctx.Err() == nil {
		m.announced = true
	}
	complete := m.announced
	m.mu.Unlock()
	if !complete {
		return fmt.Errorf("initial connector status interrupted")
	}
	return nil
}

// Close is idempotent. Pending reservations are released, but active
// transaction IDs remain recorded as Stopping until a verified Finish.
func (m *SessionManager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for connector := 1; connector < len(m.slots); connector++ {
		slot := &m.slots[connector]
		switch slot.state {
		case SessionReserved:
			slot.cancel()
			m.ledger.Abort(connector)
			*slot = managedSession{state: SessionAvailable}
		case SessionStarting:
			slot.cancel() // in-flight start response still owns reservation
		case SessionCharging:
			slot.state = SessionStopping
			slot.cancel()
		case SessionStopping:
			slot.cancel()
		}
	}
	stop := m.stopParent
	m.stopParent = nil
	m.mu.Unlock()
	if stop != nil {
		stop()
	}
}
