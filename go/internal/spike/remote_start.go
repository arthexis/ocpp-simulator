package spike

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
)

const (
	defaultRemotePowerKW      = 7.2
	defaultRemoteDuration     = 60 * time.Second
	defaultRemoteMeterInterval = time.Second
	maxRemoteDuration         = time.Hour
)

type RemoteChargeSettings struct {
	PowerKW       float64
	Duration      time.Duration
	MeterInterval time.Duration
}

func (o RemoteChargeSettings) normalized() (RemoteChargeSettings, error) {
	if o.PowerKW == 0 {
		o.PowerKW = defaultRemotePowerKW
	}
	if o.Duration == 0 {
		o.Duration = defaultRemoteDuration
	}
	if o.MeterInterval == 0 {
		o.MeterInterval = defaultRemoteMeterInterval
	}
	if math.IsNaN(o.PowerKW) || math.IsInf(o.PowerKW, 0) || o.PowerKW <= 0 ||
		o.Duration <= 0 || o.Duration > maxRemoteDuration ||
		o.MeterInterval <= 0 || o.MeterInterval > o.Duration ||
		o.PowerKW*1000*o.Duration.Seconds()/3600 > float64(1<<31-1) {
		return o, fmt.Errorf("invalid bounded remote charging settings")
	}
	return o, nil
}

type remoteStartJob struct {
	connector int
	rfid      string
	ctx       context.Context
}

// RemoteStartDispatcher owns a finite queue and at most one worker per connector.
// OnRemoteStartTransaction only reserves/queues: it never calls OCPP synchronously.
// RemoteStopTransaction and Reset remain rejected until R3/R5.
type RemoteStartDispatcher struct {
	mu        sync.Mutex
	sessions  *SessionManager
	cp        ChargePoint
	settings  RemoteChargeSettings
	jobs      chan remoteStartJob
	stop      chan struct{}
	done      chan struct{}
	errors    chan error
	ready     bool
	closed    bool
	started   bool
}

func NewRemoteStartDispatcher(sessions *SessionManager, cp ChargePoint, settings RemoteChargeSettings) (*RemoteStartDispatcher, error) {
	if sessions == nil || cp == nil {
		return nil, fmt.Errorf("remote dispatcher requires sessions and OCPP charge point")
	}
	settings, err := settings.normalized()
	if err != nil {
		return nil, err
	}
	return &RemoteStartDispatcher{
		sessions: sessions,
		cp:       cp,
		settings: settings,
		jobs:     make(chan remoteStartJob, len(sessions.slots)-1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		errors:   make(chan error, len(sessions.slots)),
	}, nil
}

// Enable is called strictly after all initial Available notifications succeeded.
func (d *RemoteStartDispatcher) Enable() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.ready || d.sessions.parent.Err() != nil {
		return fmt.Errorf("remote dispatcher cannot be enabled")
	}
	d.ready = true
	d.started = true
	go d.runQueue()
	return nil
}

func validRemoteRFID(tag string) bool {
	if tag == "" || tag != strings.TrimSpace(tag) || !utf8.ValidString(tag) ||
		utf8.RuneCountInString(tag) > 20 {
		return false
	}
	for _, r := range tag {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// TryStart is an admission operation; Accepted guarantees only bounded local
// scheduling, NOT Authorize or StartTransaction acceptance from the CSMS.
func (d *RemoteStartDispatcher) TryStart(request *core.RemoteStartTransactionRequest) bool {
	if request == nil || !validRemoteRFID(request.IdTag) || request.ChargingProfile != nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || !d.ready || d.sessions.parent.Err() != nil {
		return false
	}
	var connector int
	var ctx context.Context
	var err error
	if request.ConnectorId != nil {
		connector = *request.ConnectorId
		ctx, err = d.sessions.Reserve(connector)
	} else {
		connector, ctx, err = d.sessions.ReserveAny()
	}
	if err != nil {
		return false
	}
	job := remoteStartJob{connector: connector, rfid: request.IdTag, ctx: ctx}
	select {
	case d.jobs <- job:
		return true
	default:
		// Do not acknowledge a start whose dispatch could not be queued.
		_ = d.sessions.Abort(connector)
		return false
	}
}

func (d *RemoteStartDispatcher) report(err error) {
	if err == nil {
		return
	}
	select {
	case d.errors <- err:
	default:
		// The channel is bounded; session state still retains any unknown tx ID.
	}
}

func (d *RemoteStartDispatcher) Errors() <-chan error { return d.errors }
func (d *RemoteStartDispatcher) Done() <-chan struct{} { return d.done }

func (d *RemoteStartDispatcher) runQueue() {
	defer close(d.done)
	var workers sync.WaitGroup
	for {
		select {
		case <-d.stop:
			// Close canceled all sessions. Queued jobs are not started.
			workers.Wait()
			return
		case job := <-d.jobs:
			if job.ctx.Err() != nil {
				_ = d.sessions.Abort(job.connector)
				continue
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				err := RunRemoteCharge(job.ctx, d.cp, d.sessions, ChargeOptions{
					Connector: job.connector,
					RFID: job.rfid,
					PowerKW: d.settings.PowerKW,
					Duration: d.settings.Duration,
					MeterInterval: d.settings.MeterInterval,
				})
				if err != nil {
					d.report(fmt.Errorf("connector %d: %w", job.connector, err))
				}
			}()
		}
	}
}

// Close denies new commands and cancels workers before the OCPP client stops.
func (d *RemoteStartDispatcher) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.ready = false
	close(d.stop)
	d.sessions.Close()
	if !d.started {
		close(d.done)
	}
	d.mu.Unlock()
}

func remoteAvailable(cp ChargePoint, connector int) error {
	_, err := cp.StatusNotification(connector, core.NoError, core.ChargePointStatusAvailable)
	return err
}

// RunRemoteCharge runs only an admitted local session. RemoteStop remains
// unsupported in R2, so the session has a strict finite duration. No fake
// transaction IDs, StopTransaction acknowledgements or Available states.
func RunRemoteCharge(ctx context.Context, cp ChargePoint, sessions *SessionManager, opts ChargeOptions) error {
	if err := opts.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		_ = sessions.Abort(opts.Connector)
		return err
	}
	// A worker stays Reserved until the CSMS has explicitly accepted its
	// StartTransaction. A canceled context before this point cannot start it.
	if _, err := cp.StatusNotification(opts.Connector, core.NoError, core.ChargePointStatusPreparing); err != nil {
		_ = sessions.Abort(opts.Connector)
		return fmt.Errorf("Preparing: %w", err)
	}
	if err := ctx.Err(); err != nil {
		_ = remoteAvailable(cp, opts.Connector)
		_ = sessions.Abort(opts.Connector)
		return err
	}
	authorized, err := cp.Authorize(opts.RFID)
	if err != nil {
		_ = sessions.Abort(opts.Connector)
		return fmt.Errorf("Authorize: %w", err)
	}
	if authorized == nil || authorized.IdTagInfo == nil || authorized.IdTagInfo.Status != types.AuthorizationStatusAccepted {
		statusErr := remoteAvailable(cp, opts.Connector)
		_ = sessions.Abort(opts.Connector)
		if statusErr != nil {
			return fmt.Errorf("authorization denied and Available failed: %w", statusErr)
		}
		return fmt.Errorf("rfid_not_authorized")
	}
	if err := ctx.Err(); err != nil {
		_ = remoteAvailable(cp, opts.Connector)
		_ = sessions.Abort(opts.Connector)
		return err
	}
	if err := sessions.BeginStart(opts.Connector); err != nil {
		_ = remoteAvailable(cp, opts.Connector)
		_ = sessions.Abort(opts.Connector)
		return fmt.Errorf("begin start: %w", err)
	}
	started, err := cp.StartTransaction(opts.Connector, opts.RFID, 0, types.Now())
	if err != nil {
		// No confirmation: the CSMS may already have a transaction. Keep
		// Starting reserved until shutdown; never claim a verified closure.
		_ = sessions.RequestStop(opts.Connector)
		return fmt.Errorf("StartTransaction unconfirmed: %w", err)
	}
	if started == nil || started.IdTagInfo == nil || started.IdTagInfo.Status != types.AuthorizationStatusAccepted {
		_ = sessions.FailStart(opts.Connector)
		statusErr := remoteAvailable(cp, opts.Connector)
		if statusErr != nil {
			return fmt.Errorf("StartTransaction rejected; Available: %w", statusErr)
		}
		return fmt.Errorf("start_transaction_rejected")
	}
	txID := started.TransactionId
	if err := sessions.Start(opts.Connector, txID); err != nil {
		// The CSMS acknowledged a start but this ID could not be recorded:
		// retain the pending slot rather than invent a closure.
		return fmt.Errorf("cannot record CSMS transaction %d: %w", txID, err)
	}
	if ctx.Err() == nil {
		if _, err := cp.StatusNotification(opts.Connector, core.NoError, core.ChargePointStatusCharging); err != nil {
			_ = sessions.RequestStop(opts.Connector)
			return fmt.Errorf("Charging status failed; transaction %d may remain open: %w", txID, err)
		}
	}
	// Current R2 has finite non-battery sessions; R4 adds individual batteries.
	// Use deterministic elapsed intervals, not scheduler jitter, for Wh.
	var elapsed time.Duration
	var meterWh int
	for elapsed < opts.Duration && ctx.Err() == nil {
		step := opts.MeterInterval
		if remaining := opts.Duration-elapsed; remaining < step {
			step = remaining
		}
		if !waitStep(ctx, step) {
			break
		}
		elapsed += step
		meterWh = EnergyWh(opts.PowerKW, elapsed)
		values := []types.MeterValue{{
			Timestamp: types.Now(),
			SampledValue: []types.SampledValue{{
				Value: fmt.Sprintf("%d", meterWh),
				Measurand: types.MeasurandEnergyActiveImportRegister,
				Unit: types.UnitOfMeasureWh,
			}},
		}}
		if _, err := cp.MeterValues(opts.Connector, values, func(r *core.MeterValuesRequest) { r.TransactionId = &txID }); err != nil {
			_ = sessions.RequestStop(opts.Connector)
			return fmt.Errorf("MeterValues unconfirmed; transaction %d may remain open: %w", txID, err)
		}
	}
	_ = sessions.RequestStop(opts.Connector)
	if _, err := cp.StopTransaction(meterWh, types.Now(), txID, func(r *core.StopTransactionRequest) {
		r.Reason = core.ReasonLocal
	}); err != nil {
		return fmt.Errorf("StopTransaction unconfirmed; transaction %d may remain open: %w", txID, err)
	}
	if err := remoteAvailable(cp, opts.Connector); err != nil {
		return fmt.Errorf("StopTransaction confirmed, Available failed for transaction %d: %w", txID, err)
	}
	if err := sessions.Finish(opts.Connector, txID); err != nil {
		return fmt.Errorf("Finish transaction %d: %w", txID, err)
	}
	return nil
}
