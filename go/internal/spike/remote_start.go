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
	BatteryKWh float64
	BatterySOC float64
	BatterySeed uint64
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

	if math.IsNaN(o.BatteryKWh) || math.IsInf(o.BatteryKWh, 0) || o.BatteryKWh < 0 ||
		math.IsNaN(o.BatterySOC) || math.IsInf(o.BatterySOC, 0) {
		return o, fmt.Errorf("invalid remote battery parameters")
	}
	if o.BatteryKWh == 0 {
		if o.BatterySOC != 0 || o.BatterySeed != 0 {
			return o, fmt.Errorf("battery SOC and seed require a positive capacity")
		}
	} else {
		// A tiny interval can hide a huge power behind a small Wh total;
		// validate the maximum instantaneous OCPP W measurand too.
		if o.PowerKW*1060 > float64(1<<31-1) {
			return o, fmt.Errorf("battery power exceeds supported OCPP W range")
		}
		if _, err := NewBattery(o.BatteryKWh, o.BatterySOC); err != nil {
			return o, err
		}
	}
	return o, nil
}

// batterySlot serializes access to one physical simulated EV state. Its energy
// persists across sequential remote transactions, while meters remain relative
// to each new transaction. Distinct slots never share Battery pointers.
type remoteBatterySlot struct {
	mu sync.Mutex
	state Battery
}

func (b *remoteBatterySlot) snapshot() Battery {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *remoteBatterySlot) full() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.Full()
}

// advance is the sole writer of this connector's battery. It returns a meter
// relative to the start of this transaction, instantaneous W and current SoC.
func (b *remoteBatterySlot) advance(maxKW float64, step time.Duration, initialWh float64) (int, int, float64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delivered, err := b.state.AdvanceSeeded(maxKW, step)
	if err != nil { return 0, 0, 0, err }
	meter, err := b.state.MeterWh(initialWh)
	if err != nil { return 0, 0, 0, err }
	if step <= 0 { return 0, 0, 0, fmt.Errorf("battery meter step must be positive") }
	liveW := math.RoundToEven(delivered * 3600 / step.Seconds())
	if math.IsInf(liveW, 0) || math.IsNaN(liveW) || liveW < 0 || liveW > float64(1<<31-1) {
		return 0, 0, 0, fmt.Errorf("battery power meter exceeds OCPP range")
	}
	return meter, int(liveW), b.state.SOC(), nil
}

// splitmix64 ensures independent deterministic streams across connectors.
// In particular, connector N cannot consume or perturb connector M's seed.
func remoteConnectorSeed(base uint64, connector int) uint64 {
	z := base + uint64(connector)*0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

type remoteStartJob struct {
	connector int
	rfid      string
	ctx       context.Context
}

// RemoteStartDispatcher owns a finite queue and at most one worker per connector.
// OnRemoteStartTransaction only reserves/queues: it never calls OCPP synchronously.
// RemoteStopTransaction cancels a known session; Reset remains rejected until R5.
type RemoteStartDispatcher struct {
	mu        sync.Mutex
	sessions  *SessionManager
	cp        ChargePoint
	settings  RemoteChargeSettings
	batteries []*remoteBatterySlot
	jobs      chan remoteStartJob
	stop      chan struct{}
	done      chan struct{}
	errors    chan error
	ready     bool
	closed    bool
	started   bool
	resetEvents chan core.ResetType
}

func NewRemoteStartDispatcher(sessions *SessionManager, cp ChargePoint, settings RemoteChargeSettings) (*RemoteStartDispatcher, error) {
	if sessions == nil || cp == nil {
		return nil, fmt.Errorf("remote dispatcher requires sessions and OCPP charge point")
	}
	settings, err := settings.normalized()
	if err != nil {
		return nil, err
	}
	d := &RemoteStartDispatcher{
		sessions: sessions,
		cp:       cp,
		settings: settings,
		jobs:     make(chan remoteStartJob, len(sessions.slots)-1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		errors:   make(chan error, len(sessions.slots)),
		resetEvents: make(chan core.ResetType,1),
	}
	if settings.BatteryKWh > 0 {
		d.batteries = make([]*remoteBatterySlot, len(sessions.slots))
		for connector := 1; connector < len(sessions.slots); connector++ {
			battery, err := NewBattery(settings.BatteryKWh, settings.BatterySOC)
			if err != nil { return nil, err }
			battery.SetSeed(remoteConnectorSeed(settings.BatterySeed, connector))
			d.batteries[connector] = &remoteBatterySlot{state:battery}
		}
	}
	return d, nil
}

// batteryFor returns connector-owned state; each worker has exclusive charging
// ownership, and the slot mutex protects concurrent observations/admissions.
func (d *RemoteStartDispatcher) batteryFor(connector int) *remoteBatterySlot {
	if len(d.batteries) == 0 || connector < 1 || connector >= len(d.batteries) {
		return nil
	}
	return d.batteries[connector]
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
		if b := d.batteryFor(connector); b != nil && b.full() { return false }
		ctx, err = d.sessions.Reserve(connector)
	} else if len(d.batteries) != 0 {
		// Skip exhausted batteries in automatic connector selection. The
		// manager still performs the atomic reservation under its own lock.
		for candidate := 1; candidate < len(d.sessions.slots); candidate++ {
			if d.batteries[candidate].full() { continue }
			if reserved, reserveErr := d.sessions.Reserve(candidate); reserveErr == nil {
				connector, ctx = candidate, reserved
				break
			}
		}
		if ctx == nil { return false }
	} else {
		connector, ctx, err = d.sessions.ReserveAny()
	}
	if err != nil {return false}
 // Before emitting Accepted, freeze battery SoC/seed together with the
 // connector's durable reservation. An uncertain write cannot dispatch.
 if battery:=d.batteryFor(connector);battery!=nil {
  if err:=d.sessions.InitializeBattery(connector,snapshotBattery(battery));err!=nil{return false}
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

// TryStop only admits a CSMS-assigned ID still owned by an active session.
// Accepted acknowledges the cancellation request, NOT the StopTransaction
// confirmation. The session worker remains the sole OCPP stop sender.
func (d *RemoteStartDispatcher) TryStop(request *core.RemoteStopTransactionRequest) bool {
	if request == nil || request.TransactionId <= 0 {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || !d.ready || d.sessions.parent.Err() != nil {
		return false
	}
	_, err := d.sessions.RequestRemoteStop(request.TransactionId)
	return err == nil
}

// TryReset atomically blocks new starts/stops and cancels connector contexts.
// The OCPP handler only enqueues a reset notification: network teardown must
// happen in the Serve loop, after ResetConfirmation can be sent.
func (d *RemoteStartDispatcher) TryReset(request *core.ResetRequest) bool {
	if request == nil || (request.Type != core.ResetTypeSoft && request.Type != core.ResetTypeHard) {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || !d.ready || d.sessions.parent.Err() != nil {
		return false
	}
	if err := d.sessions.BeginReset(request.Type); err != nil {
		return false
	}
	d.ready = false
	select {
	case d.resetEvents <- request.Type:
		return true
	default:
		return false // unreachable under d.mu; fail closed
	}
}

func (d *RemoteStartDispatcher) ResetEvents() <-chan core.ResetType {
	return d.resetEvents
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
				err := runRemoteCharge(job.ctx, d.cp, d.sessions, ChargeOptions{
					Connector: job.connector,
					RFID: job.rfid,
					PowerKW: d.settings.PowerKW,
					Duration: d.settings.Duration,
					MeterInterval: d.settings.MeterInterval,
				}, d.batteryFor(job.connector))
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

// RunRemoteCharge runs only an admitted local session. RemoteStop can
// request cancellation for a recognized CSMS transaction; it cannot
// fabricate StopTransaction acknowledgements or Available states.
func RunRemoteCharge(ctx context.Context, cp ChargePoint, sessions *SessionManager, opts ChargeOptions) error {
	return runRemoteCharge(ctx, cp, sessions, opts, nil)
}

func runRemoteCharge(ctx context.Context, cp ChargePoint, sessions *SessionManager, opts ChargeOptions, battery *remoteBatterySlot) error {
	if err := opts.Validate(); err != nil {
		return err
	}
	if battery != nil && battery.full() {
		_ = sessions.Abort(opts.Connector)
		return fmt.Errorf("connector %d battery is full", opts.Connector)
	}
	if sessions.IsHardReset() { return fmt.Errorf("hard reset interrupted connector %d", opts.Connector) }
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
	if sessions.IsHardReset() { return fmt.Errorf("hard reset interrupted connector %d after Preparing", opts.Connector) }
	if err := ctx.Err(); err != nil {
		_ = remoteAvailable(cp, opts.Connector)
		_ = sessions.Abort(opts.Connector)
		return err
	}
	authorized, err := cp.Authorize(opts.RFID)
	if sessions.IsHardReset() { return fmt.Errorf("hard reset interrupted connector %d after Authorize", opts.Connector) }
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
	if sessions.IsHardReset() { return fmt.Errorf("hard reset interrupted connector %d before StartTransaction", opts.Connector) }
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
	if sessions.IsHardReset() {
		// Record any definitive late ID but never initiate a fabricated stop.
		if err == nil && started != nil && started.IdTagInfo != nil &&
			started.IdTagInfo.Status == types.AuthorizationStatusAccepted {
			_ = sessions.Start(opts.Connector, started.TransactionId)
		}
		return fmt.Errorf("hard reset interrupted StartTransaction on connector %d", opts.Connector)
	}
	if err != nil {
		// No confirmation: the CSMS may already have a transaction. Keep
		// Starting reserved until shutdown; never claim a verified closure.
		_ = sessions.RequestStop(opts.Connector)
		return fmt.Errorf("StartTransaction unconfirmed: %w", err)
	}
	if started == nil {
		_ = sessions.RequestStop(opts.Connector)
		return fmt.Errorf("StartTransaction missing confirmation; connector %d remains reserved", opts.Connector)
	}
	if started.IdTagInfo == nil || started.IdTagInfo.Status != types.AuthorizationStatusAccepted {
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
	if sessions.IsHardReset() { return fmt.Errorf("hard reset interrupted transaction %d", txID) }
	if ctx.Err() == nil {
		if _, err := cp.StatusNotification(opts.Connector, core.NoError, core.ChargePointStatusCharging); err != nil {
			_ = sessions.RequestStop(opts.Connector)
			return fmt.Errorf("Charging status failed; transaction %d may remain open: %w", txID, err)
		}
	}
	if sessions.IsHardReset() { return fmt.Errorf("hard reset interrupted transaction %d after Charging", txID) }
	// Meter energy is relative to this transaction even when the connector's
	// battery carries prior-session energy. Elapsed time is deterministic.
	var initialBatteryWh float64
	if battery != nil { initialBatteryWh = battery.snapshot().EnergyWh }
	var elapsed time.Duration
	var meterWh int
	for elapsed < opts.Duration && ctx.Err() == nil && (battery == nil || !battery.full()) {
		step := opts.MeterInterval
		if remaining := opts.Duration-elapsed; remaining < step {
			step = remaining
		}
		if !waitStep(ctx, step) { break }
		if sessions.IsHardReset() { return fmt.Errorf("hard reset interrupted transaction %d during metering", txID) }
		// Cancellation between the timer and energy integration must not
		// consume an additional full interval after a RemoteStop request.
		if ctx.Err() != nil { break }
		elapsed += step
		var samples []types.SampledValue
  var meterWatts int
		if battery != nil {
			wh, watts, soc, err := battery.advance(opts.PowerKW, step, initialBatteryWh)
			if err != nil {
				_ = sessions.RequestStop(opts.Connector)
				return fmt.Errorf("battery meter invalid; transaction %d may remain open: %w", txID, err)
			}
			meterWh=wh
   meterWatts=watts
			samples = []types.SampledValue{
				{Value: fmt.Sprintf("%d", wh), Measurand: types.MeasurandEnergyActiveImportRegister, Unit: types.UnitOfMeasureWh},
				{Value: fmt.Sprintf("%d", watts), Measurand: types.MeasurandPowerActiveImport, Unit: types.UnitOfMeasureW},
				{Value: fmt.Sprintf("%.2f", soc), Measurand: types.MeasurandSoC, Unit: types.UnitOfMeasurePercent},
			}
		} else {
			meterWh=EnergyWh(opts.PowerKW,elapsed)
   meterWatts=int(math.RoundToEven(opts.PowerKW*1000))
			samples = []types.SampledValue{{Value:fmt.Sprintf("%d", meterWh), Measurand:types.MeasurandEnergyActiveImportRegister, Unit:types.UnitOfMeasureWh}}
		}
		values:=[]types.MeterValue{{Timestamp:types.Now(),SampledValue:samples}}
  if sessions.IsHardReset(){return fmt.Errorf("hard reset before MeterValues on %d",txID)}
  if err:=sessions.RecordMeterIntent(opts.Connector,txID,meterWh,meterWatts,snapshotBattery(battery));err!=nil{
   return fmt.Errorf("MeterValues not sent; durable intent failed: %w",err)
  }
  if sessions.IsHardReset(){return fmt.Errorf("hard reset after durable MeterValues intent on %d",txID)}
  if ctx.Err()!=nil{break} // do not send after an accepted RemoteStop
  if _,err:=cp.MeterValues(opts.Connector,values,func(r *core.MeterValuesRequest){r.TransactionId=&txID});err!=nil{
   _=sessions.RequestStop(opts.Connector)
   return fmt.Errorf("MeterValues unconfirmed; transaction %d may remain open: %w",txID,err)
  }
  if err:=sessions.RecordMeterConfirmation(opts.Connector,txID);err!=nil{
   return fmt.Errorf("MeterValues ACK not durable; transaction %d remains uncertain: %w",txID,err)
  }
	}
	if sessions.IsHardReset() { return fmt.Errorf("hard reset forbids StopTransaction on %d", txID) }
	reason, err := sessions.BeginStopReasonWithMeter(opts.Connector, txID,meterWh)
	if err != nil {
		// No StopTransaction has been sent. Do not claim completion.
		return fmt.Errorf("cannot claim StopTransaction %d: %w", txID, err)
	}
	if sessions.IsHardReset() { return fmt.Errorf("hard reset interrupted claimed StopTransaction %d", txID) }
	confirmation, err := cp.StopTransaction(meterWh, types.Now(), txID, func(r *core.StopTransactionRequest) {
		r.Reason = reason
	})
	if err != nil {
		return fmt.Errorf("StopTransaction unconfirmed; transaction %d may remain open: %w", txID, err)
	}
	if confirmation == nil {return fmt.Errorf("StopTransaction missing confirmation; transaction %d may remain open",txID)}
 if err:=sessions.RecordStopConfirmation(opts.Connector,txID);err!=nil{
  return fmt.Errorf("StopTransaction ACK not durable; retain transaction %d: %w",txID,err)
 }
	if sessions.IsHardReset() {
		return fmt.Errorf("hard reset after StopTransaction response; do not announce Available for %d", txID)
	}
	if err := remoteAvailable(cp, opts.Connector); err != nil {
		return fmt.Errorf("StopTransaction confirmed, Available failed for transaction %d: %w", txID, err)
	}
	if err := sessions.Finish(opts.Connector, txID); err != nil {
		return fmt.Errorf("Finish transaction %d: %w", txID, err)
	}
	return nil
}
