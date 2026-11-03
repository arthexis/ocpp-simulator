package spike

import (
	"context"
	"fmt"
	"math"
	"time"

	ocpp16 "github.com/lorenzodonini/ocpp-go/ocpp1.6"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
)

// ChargeOptions describes a single connector, one-shot transaction.
type ChargeOptions struct {
	Connector int
	RFID string
	PowerKW float64
	Duration time.Duration
	MeterInterval time.Duration
}

// EnergyWh computes cumulative meter energy from simulated elapsed time, not
// wall-clock callback timing. Go's math.Round matches Python's decimal
// rounding for non-tie positive values; tie cases use half-away-from-zero.
func EnergyWh(powerKW float64, elapsed time.Duration) int {
	return int(math.Round(powerKW * 1000 * elapsed.Seconds() / 3600))
}

func (o ChargeOptions) Validate() error {
	if o.Connector < 1 || len(o.RFID) < 1 || len(o.RFID) > 20 ||
		math.IsNaN(o.PowerKW) || math.IsInf(o.PowerKW, 0) || o.PowerKW <= 0 ||
		o.Duration < 0 || o.MeterInterval <= 0 || math.IsInf(o.MeterInterval.Seconds(), 0) ||
		math.IsInf(o.Duration.Seconds(), 0) {
		return fmt.Errorf("invalid one-shot charging parameters")
	}
	if o.PowerKW*1000*o.Duration.Seconds()/3600 > float64(math.MaxInt32) {
		return fmt.Errorf("energy exceeds supported OCPP meter range")
	}
	return nil
}

// ChargePoint deliberately keeps the boundary at ocpp-go's charge point API.
// A fake can implement this interface for deterministic protocol tests.
type ChargePoint interface {
	StatusNotification(int, core.ChargePointErrorCode, core.ChargePointStatus, ...func(*core.StatusNotificationRequest)) (*core.StatusNotificationConfirmation, error)
	Authorize(string, ...func(*core.AuthorizeRequest)) (*core.AuthorizeConfirmation, error)
	StartTransaction(int, string, int, *types.DateTime, ...func(*core.StartTransactionRequest)) (*core.StartTransactionConfirmation, error)
	MeterValues(int, []types.MeterValue, ...func(*core.MeterValuesRequest)) (*core.MeterValuesConfirmation, error)
	StopTransaction(int, *types.DateTime, int, ...func(*core.StopTransactionRequest)) (*core.StopTransactionConfirmation, error)
}

var _ ChargePoint = (ocpp16.ChargePoint)(nil)

func waitStep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done(): return false
	case <-timer.C: return true
	}
}

// Charge sends exactly one transaction. If interrupted after a successful
// StartTransaction, it attempts StopTransaction before reporting Available.
// If the link breaks, the caller must not report an invented close.
func Charge(ctx context.Context, cp ChargePoint, o ChargeOptions) (int, error) {
	if err := o.Validate(); err != nil { return 0, err }
	available := func() error {
		_, err := cp.StatusNotification(o.Connector, core.NoError, core.ChargePointStatusAvailable)
		if err != nil { return fmt.Errorf("Available: %w",err) }
		return nil
	}
	if _, err := cp.StatusNotification(o.Connector, core.NoError, core.ChargePointStatusPreparing); err != nil {
		return 0, fmt.Errorf("Preparing: %w",err)
	}
	authorized, err := cp.Authorize(o.RFID)
	if err != nil { return 0, fmt.Errorf("Authorize: %w",err) }
	if authorized == nil || authorized.IdTagInfo == nil || authorized.IdTagInfo.Status != types.AuthorizationStatusAccepted {
		_ = available()
		return 0, fmt.Errorf("rfid_not_authorized")
	}
	started, err := cp.StartTransaction(o.Connector, o.RFID, 0, types.Now())
	if err != nil { return 0, fmt.Errorf("StartTransaction: %w",err) }
	if started == nil || started.IdTagInfo == nil || started.IdTagInfo.Status != types.AuthorizationStatusAccepted {
		_ = available()
		return 0, fmt.Errorf("start_transaction_rejected")
	}
	txID := started.TransactionId
	if _, err := cp.StatusNotification(o.Connector, core.NoError, core.ChargePointStatusCharging); err != nil {
		return 0, fmt.Errorf("Charging: %w (transaction %d may remain open)",err,txID)
	}

	var elapsed time.Duration
	var wh int
	var meterErr error
	for elapsed < o.Duration {
		step := o.MeterInterval
		if remaining := o.Duration - elapsed; remaining < step { step = remaining }
		if !waitStep(ctx,step) { break }
		elapsed += step
		wh = EnergyWh(o.PowerKW,elapsed)
		sample := types.SampledValue{Value:fmt.Sprintf("%d",wh),Measurand:types.MeasurandEnergyActiveImportRegister,Unit:types.UnitOfMeasureWh}
		values := []types.MeterValue{{Timestamp:types.Now(),SampledValue:[]types.SampledValue{sample}}}
		_, err := cp.MeterValues(o.Connector,values,func(r *core.MeterValuesRequest){r.TransactionId=&txID})
		if err != nil {
			meterErr = fmt.Errorf("MeterValues: %w (transaction %d may remain open)",err,txID)
			break
		}
		fmt.Printf("Meter: connector %d, %d Wh\n",o.Connector,wh)
	}
	// A failed MeterValues request can signal transport loss; don't pretend
	// StopTransaction succeeded. Preserve recovery evidence at the CSMS.
	if meterErr != nil { return wh,meterErr }
	_, err = cp.StopTransaction(wh,types.Now(),txID,func(r *core.StopTransactionRequest){r.Reason=core.ReasonLocal})
	if err != nil { return wh,fmt.Errorf("StopTransaction: %w (transaction %d may remain open)",err,txID) }
	if err := available(); err != nil { return wh,err }
	fmt.Printf("Transaction complete: %d Wh (id %d)\n",wh,txID)
	return wh,nil
}
