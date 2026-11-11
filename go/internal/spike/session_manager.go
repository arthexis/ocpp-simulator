package spike

import (
	"context"
	"fmt"
	"sync"
 "time"

	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
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
	remoteStop    bool
	stopIssued    bool
 attemptID string
	ctx           context.Context
	cancel        context.CancelFunc
}

// SessionManager bounds ownership and transaction identity per connector.
// A pending StartTransaction stays reserved until its response is known.
type SessionManager struct {
	mu         sync.Mutex
	parent     context.Context
	ledger     *ConnectorLedger
 journal RecoveryJournal
	slots      []managedSession // connector IDs start at 1; slot 0 is unused
	closed     bool
	resetMode  core.ResetType
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

// The journal is bound only within this Go package's offline recovery tests.
// Production Serve still refuses every remote connection without CSMS authority.
func newSessionManagerWithRecovery(parent context.Context,connectors int,journal RecoveryJournal)(*SessionManager,error){
 if journal==nil{return nil,fmt.Errorf("journal required")}
 m,err:=NewSessionManager(parent,connectors)
 if err!=nil{return nil,err}
 m.mu.Lock();m.journal=journal;m.mu.Unlock()
 return m,nil
}
func (m *SessionManager) persist(fn func(context.Context)error)error{
 if m.journal==nil{return nil}
 ctx,cancel:=context.WithTimeout(context.Background(),3*time.Second)
 defer cancel()
 return fn(ctx)
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
	if err := m.ledger.Reserve(connector); err != nil {return nil,err}
 var attempt string
 if err:=m.persist(func(ctx context.Context)error{
  var e error;attempt,e=m.journal.ReserveIntent(ctx,connector);return e
 });err!=nil{m.ledger.Abort(connector);return nil,fmt.Errorf("durable reservation: %w",err)}
 ctx,cancel:=context.WithCancel(m.parent)
 m.slots[connector]=managedSession{state:SessionReserved,ctx:ctx,cancel:cancel,attemptID:attempt}
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
		if err:=m.ledger.Reserve(connector);err!=nil{return 0,nil,err}
  var attempt string
  if err:=m.persist(func(ctx context.Context)error{
   var e error;attempt,e=m.journal.ReserveIntent(ctx,connector);return e
  });err!=nil{m.ledger.Abort(connector);return 0,nil,fmt.Errorf("durable reservation: %w",err)}
  ctx,cancel:=context.WithCancel(m.parent)
  m.slots[connector]=managedSession{state:SessionReserved,ctx:ctx,cancel:cancel,attemptID:attempt}
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
	if err:=m.persist(func(ctx context.Context)error{return m.journal.StartIntent(ctx,slot.attemptID,connector)});err!=nil{
  return fmt.Errorf("durable start intent: %w",err)
 }
 slot.state=SessionStarting
 return nil
}

// FailStart releases a pending StartTransaction only after the CSMS has
// explicitly rejected it or its request has failed without an accepted ID.
// A transport timeout can be ambiguous and must be treated by the caller as
// an unknown transaction rather than a successful closure.
func (m *SessionManager) FailStart(connector int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.validConnector(connector)||m.slots[connector].state!=SessionStarting{return fmt.Errorf("connector %d has no pending start",connector)}
 slot:=&m.slots[connector]
 if err:=m.persist(func(ctx context.Context)error{return m.journal.StartRejected(ctx,slot.attemptID,connector)});err!=nil{
  return fmt.Errorf("durable start rejected: %w",err)
 }
 slot.cancel()
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
	if transactionID<=0{return fmt.Errorf("CSMS ID must be positive")}
 if owner,ok:=m.ledger.ConnectorFor(transactionID);ok&&owner!=connector{return fmt.Errorf("duplicate CSMS ID")}
 if err:=m.persist(func(ctx context.Context)error{return m.journal.StartConfirmed(ctx,slot.attemptID,connector,transactionID)});err!=nil{
  return fmt.Errorf("durable CSMS start ACK: %w",err)
 }
 if err:=m.ledger.Start(connector,transactionID);err!=nil{return err}
 if m.closed||m.parent.Err()!=nil||slot.ctx.Err()!=nil {
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
	if !m.validConnector(connector)||m.slots[connector].state!=SessionReserved{return fmt.Errorf("connector %d has no abortable reservation",connector)}
 slot:=&m.slots[connector]
 if err:=m.persist(func(ctx context.Context)error{return m.journal.AbortIntent(ctx,slot.attemptID,connector)});err!=nil{
  return fmt.Errorf("durable reservation abort: %w",err)
 }
 slot.cancel()
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

// RequestRemoteStop applies only to an active, CSMS-acknowledged transaction.
// It holds the manager lock across ID lookup, state check, and cancellation,
// preventing Finish or connector reuse from racing with RemoteStop.
// Repeating the same request is idempotent while that transaction is retained.
func (m *SessionManager) RequestRemoteStop(transactionID int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if transactionID <= 0 || m.closed || m.parent.Err() != nil {
		return 0, fmt.Errorf("invalid or unavailable remote transaction %d", transactionID)
	}
	connector, ok := m.ledger.ConnectorFor(transactionID)
	if !ok || !m.validConnector(connector) {
		return 0, fmt.Errorf("unknown remote transaction %d", transactionID)
	}
	slot := &m.slots[connector]
	if slot.transactionID != transactionID {
		return 0, fmt.Errorf("stale transaction %d", transactionID)
	}
	if slot.remoteStop && slot.state == SessionStopping {
		return connector, nil
	}
	if slot.state!=SessionCharging||slot.stopIssued{return 0,fmt.Errorf("transaction %d no longer accepting remote stop",transactionID)}
 if err:=m.persist(func(ctx context.Context)error{return m.journal.RequestRemoteStopIntent(ctx,slot.attemptID,connector,transactionID)});err!=nil{
  return 0,fmt.Errorf("durable RemoteStop request: %w",err)
 }
 slot.remoteStop=true
	slot.state = SessionStopping
	slot.cancel()
	return connector, nil
}

// BeginStopReason claims the single StopTransaction send and freezes its
// reason under the session lock. Hard reset prohibits new OCPP stop calls;
// Soft reset uses the OCPP SoftReset reason unless a RemoteStop was already
// accepted. A previously claimed reason never changes.
func(m *SessionManager) BeginStopReason(connector,transactionID int)(core.Reason,error){
 return m.BeginStopReasonWithMeter(connector,transactionID,0)
}
func (m *SessionManager) BeginStopReasonWithMeter(connector, transactionID,meterWh int)(core.Reason,error){
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.validConnector(connector) || transactionID <= 0 {
		return "", fmt.Errorf("invalid stop identity")
	}
	slot := &m.slots[connector]
	if m.resetMode == core.ResetTypeHard ||
		slot.transactionID != transactionID ||
		(slot.state != SessionCharging && slot.state != SessionStopping) ||
		slot.stopIssued {
		return "", fmt.Errorf("transaction %d cannot claim a new stop", transactionID)
	}
	reason := core.ReasonLocal
	if slot.remoteStop {
		reason = core.ReasonRemote
	} else if m.resetMode == core.ResetTypeSoft {
		reason = core.ReasonSoftReset
	}
	if err:=m.persist(func(ctx context.Context)error{return m.journal.StopIntent(ctx,slot.attemptID,connector,transactionID,reason,meterWh)});err!=nil{
  return "",fmt.Errorf("durable StopTransaction intent: %w",err)
 }
 slot.state=SessionStopping
 slot.stopIssued=true
 return reason,nil
}

// BeginStop remains compatible with R3's boolean remote-reason contract.
func (m *SessionManager) BeginStop(connector, transactionID int) (bool, error) {
	reason, err := m.BeginStopReason(connector, transactionID)
	return reason == core.ReasonRemote, err
}

// IsHardReset is checked before new OCPP calls. It cannot revoke an in-flight
// WebSocket call, so late or ambiguous responses remain fail-closed.
func (m *SessionManager) IsHardReset() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resetMode == core.ResetTypeHard
}

func (m *SessionManager) ResetMode() core.ResetType {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resetMode
}

// Unresolved snapshots pending and acknowledged transaction ownership. The
// caller must log these before exiting; no durable replay exists in R5.
func (m *SessionManager) Unresolved() []SessionSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	var unresolved []SessionSnapshot
	for connector := 1; connector < len(m.slots); connector++ {
		slot := m.slots[connector]
		if slot.state != SessionAvailable {
			unresolved = append(unresolved, SessionSnapshot{
				Connector: connector, State: slot.state, TransactionID: slot.transactionID,
			})
		}
	}
	return unresolved
}

// Commit the initial connector battery state before RemoteStart Accepted.
func(m *SessionManager) InitializeBattery(connector int,battery RecoveryBattery)error{
 m.mu.Lock();defer m.mu.Unlock()
 if !m.validConnector(connector){return fmt.Errorf("invalid battery connector")}
 slot:=&m.slots[connector]
 if slot.state!=SessionReserved{return fmt.Errorf("battery not reserved")}
 if err:=m.persist(func(ctx context.Context)error{return m.journal.BatteryInitialized(ctx,slot.attemptID,connector,battery)});err!=nil{return fmt.Errorf("durable initial battery: %w",err)}
 return nil
}
// Commit the new meter/seed/SoC before sending MeterValues.
func(m *SessionManager) RecordMeterIntent(connector,txID,meterWh,watts int,battery RecoveryBattery)error{
 m.mu.Lock();defer m.mu.Unlock()
 if !m.validConnector(connector){return fmt.Errorf("invalid meter connector")}
 slot:=&m.slots[connector]
 if slot.transactionID!=txID||(slot.state!=SessionCharging&&slot.state!=SessionStopping)||slot.stopIssued{return fmt.Errorf("meter transaction owner invalid")}
 if err:=m.persist(func(ctx context.Context)error{return m.journal.MeterIntent(ctx,slot.attemptID,connector,txID,meterWh,watts,battery)});err!=nil{
  return fmt.Errorf("durable meter intent: %w",err)
 }
 return nil
}
// Persist the MeterValues ACK before consuming another simulated interval.
func(m *SessionManager) RecordMeterConfirmation(connector,txID int)error{
 m.mu.Lock();defer m.mu.Unlock()
 if !m.validConnector(connector){return fmt.Errorf("invalid meter connector")}
 slot:=&m.slots[connector]
 if slot.transactionID!=txID{return fmt.Errorf("meter ACK owner mismatch")}
 if err:=m.persist(func(ctx context.Context)error{return m.journal.MeterConfirmed(ctx,slot.attemptID,connector,txID)});err!=nil{return fmt.Errorf("durable meter ACK: %w",err)}
 return nil
}
// Called after the CSMS's StopTransaction ACK and before Available.
func(m *SessionManager) RecordStopConfirmation(connector,txID int)error{
 m.mu.Lock();defer m.mu.Unlock()
 if !m.validConnector(connector){return fmt.Errorf("invalid stop connector")}
 slot:=&m.slots[connector]
 if slot.transactionID!=txID||!slot.stopIssued{return fmt.Errorf("stop not claimed")}
 if err:=m.persist(func(ctx context.Context)error{return m.journal.StopConfirmed(ctx,slot.attemptID,connector,txID)});err!=nil{
  return fmt.Errorf("durable StopTransaction ACK: %w",err)
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
	slot:=&m.slots[connector]
 if slot.transactionID!=transactionID{return fmt.Errorf("wrong finish transaction")}
 if err:=m.persist(func(ctx context.Context)error{return m.journal.FinishConfirmed(ctx,slot.attemptID,connector,transactionID)});err!=nil{
  return fmt.Errorf("durable close: %w",err)
 }
 if err:=m.ledger.Finish(connector,transactionID);err!=nil{return err}
 slot.cancel()
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

// shutdown atomically denies new reservations and cancels owned contexts.
// It retains both acknowledged transaction IDs and ambiguous Starting slots;
// only explicit CSMS confirmations allow Finish to clear these records.
func (m *SessionManager) shutdown(mode core.ResetType) bool {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false
	}
	m.closed = true
	m.resetMode = mode
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
	if stop != nil { stop() }
	return true
}

// BeginReset initiates one soft or hard reset; duplicate or conflicting Reset
// commands are rejected rather than silently changing the existing mode.
func (m *SessionManager) BeginReset(mode core.ResetType) error {
	if mode != core.ResetTypeSoft && mode != core.ResetTypeHard {
		return fmt.Errorf("invalid reset type %q", mode)
	}
	if !m.shutdown(mode) {
		return fmt.Errorf("reset or shutdown already in progress")
	}
	return nil
}

// Close is idempotent. An ordinary shutdown permits verified StopTransaction
// for active sessions; a Hard reset does not fabricate any shutdown receipt.
func (m *SessionManager) Close() {
	m.shutdown("")
}
