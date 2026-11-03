package spike

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
)

type fakeChargePoint struct {
	actions []string
	meter []int
	auth types.AuthorizationStatus
	start types.AuthorizationStatus
	failMeter bool
	txID int
}

func (f *fakeChargePoint) StatusNotification(_ int, _ core.ChargePointErrorCode, status core.ChargePointStatus, _ ...func(*core.StatusNotificationRequest)) (*core.StatusNotificationConfirmation,error) {
	f.actions=append(f.actions,string(status))
	return &core.StatusNotificationConfirmation{},nil
}
func (f *fakeChargePoint) Authorize(_ string,_ ...func(*core.AuthorizeRequest)) (*core.AuthorizeConfirmation,error) {
	f.actions=append(f.actions,"Authorize")
	return &core.AuthorizeConfirmation{IdTagInfo:types.NewIdTagInfo(f.auth)},nil
}
func (f *fakeChargePoint) StartTransaction(_ int,_ string,_ int,_ *types.DateTime,_ ...func(*core.StartTransactionRequest)) (*core.StartTransactionConfirmation,error) {
	f.actions=append(f.actions,"StartTransaction")
	return &core.StartTransactionConfirmation{IdTagInfo:types.NewIdTagInfo(f.start),TransactionId:42},nil
}
func (f *fakeChargePoint) MeterValues(_ int, values []types.MeterValue,props ...func(*core.MeterValuesRequest)) (*core.MeterValuesConfirmation,error) {
	f.actions=append(f.actions,"MeterValues")
	if f.failMeter {return nil,errors.New("socket lost")}
	if len(props)!=1 {return nil,fmt.Errorf("missing transaction id")}
	r:=core.NewMeterValuesRequest(1,values)
	props[0](r)
	if r.TransactionId==nil || *r.TransactionId!=42 {return nil,fmt.Errorf("wrong transaction ID")}
	var n int
	if _,err:=fmt.Sscan(values[0].SampledValue[0].Value,&n);err!=nil{return nil,err}
	f.meter=append(f.meter,n)
	return &core.MeterValuesConfirmation{},nil
}
func (f *fakeChargePoint) StopTransaction(meter int,_ *types.DateTime,txID int,_ ...func(*core.StopTransactionRequest)) (*core.StopTransactionConfirmation,error) {
	f.actions=append(f.actions,fmt.Sprintf("StopTransaction:%d:%d",txID,meter))
	return &core.StopTransactionConfirmation{},nil
}

func sampleCharge() ChargeOptions {
	return ChargeOptions{Connector:1,RFID:"TEST001",PowerKW:7.2,Duration:30*time.Millisecond,MeterInterval:10*time.Millisecond}
}

func TestEnergyExact(t *testing.T) {
	if got:=EnergyWh(7.2,60*time.Second);got!=120 {t.Fatalf("60s at 7.2kW: %d Wh",got)}
	last:=0
	for n:=1;n<=60;n++ {
		next:=EnergyWh(7.2,time.Duration(n)*time.Second)
		if next<last {t.Fatal("meter energy decreased")}
		last=next
	}
}
func TestChargeComplete(t *testing.T) {
	cp:=&fakeChargePoint{auth:types.AuthorizationStatusAccepted,start:types.AuthorizationStatusAccepted}
	wh,err:=Charge(context.Background(),cp,sampleCharge())
	if err!=nil {t.Fatal(err)}
	if wh!=0 {t.Fatalf("unexpected energy: %d",wh)}
	joined:=strings.Join(cp.actions,",")
	for _,term:=range []string{"Preparing,Authorize,StartTransaction,Charging,MeterValues","StopTransaction:42:0,Available"} {
		if !strings.Contains(joined,term) {t.Fatalf("missing %q in %s",term,joined)}
	}
	if len(cp.meter)!=3 {t.Fatalf("meter events: %d",len(cp.meter))}
}
func TestChargeRejectAuthorize(t *testing.T) {
	cp:=&fakeChargePoint{auth:types.AuthorizationStatusInvalid,start:types.AuthorizationStatusAccepted}
	_,err:=Charge(context.Background(),cp,sampleCharge())
	if err==nil||!strings.Contains(err.Error(),"rfid_not_authorized"){t.Fatal(err)}
	if strings.Contains(strings.Join(cp.actions,","),"StartTransaction"){t.Fatalf("started after rejected auth: %v",cp.actions)}
}
func TestChargeRejectStart(t *testing.T) {
	cp:=&fakeChargePoint{auth:types.AuthorizationStatusAccepted,start:types.AuthorizationStatusInvalid}
	_,err:=Charge(context.Background(),cp,sampleCharge())
	if err==nil||!strings.Contains(err.Error(),"start_transaction_rejected"){t.Fatal(err)}
	if strings.Contains(strings.Join(cp.actions,","),"Charging"){t.Fatalf("charged after rejected start: %v",cp.actions)}
}
func TestChargeInterruptedStops(t *testing.T) {
	ctx,cancel:=context.WithCancel(context.Background())
	cp:=&fakeChargePoint{auth:types.AuthorizationStatusAccepted,start:types.AuthorizationStatusAccepted}
	cancel()
	_,err:=Charge(ctx,cp,sampleCharge())
	if err!=nil {t.Fatal(err)}
	if !strings.Contains(strings.Join(cp.actions,","),"StopTransaction:42:0,Available"){t.Fatalf("did not stop: %v",cp.actions)}
}
func TestFailedMeterDoesNotInventStop(t *testing.T) {
	cp:=&fakeChargePoint{auth:types.AuthorizationStatusAccepted,start:types.AuthorizationStatusAccepted,failMeter:true}
	_,err:=Charge(context.Background(),cp,sampleCharge())
	if err==nil {t.Fatal("expected meter error")}
	joined:=strings.Join(cp.actions,",")
	if strings.Contains(joined,"StopTransaction")||strings.Contains(joined,"Available"){t.Fatalf("fabricated close: %s",joined)}
}
func TestChargeRejectsBadOptions(t *testing.T) {
	for _,invalid:=range []ChargeOptions{
		{Connector:0,RFID:"TEST001",PowerKW:7.2,Duration:time.Second,MeterInterval:time.Second},
		{Connector:1,RFID:"",PowerKW:7.2,Duration:time.Second,MeterInterval:time.Second},
		{Connector:1,RFID:"TEST001",PowerKW:-1,Duration:time.Second,MeterInterval:time.Second},
		{Connector:1,RFID:"TEST001",PowerKW:7.2,Duration:time.Second,MeterInterval:0},
	} {
		if err:=invalid.Validate();err==nil {t.Fatalf("accepted %+v",invalid)}
	}
}
