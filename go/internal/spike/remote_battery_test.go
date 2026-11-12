package spike

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
)

// batteryFixtureCP is a race-safe local OCPP fake with independent transaction
// IDs per connector. It has no networking or production dependencies.
type batteryFixtureCP struct {
	mu sync.Mutex
	next map[int]int
	active map[int]int
	samples map[int][][]types.SampledValue
	stopReason map[int]core.Reason
	stopCount map[int]int
	stopWh map[int]int
	available map[int]int
	pulse chan int
	failMeter bool
	failStop bool
}

func newBatteryFixture() *batteryFixtureCP {
	return &batteryFixtureCP{
		next:map[int]int{}, active:map[int]int{}, samples:map[int][][]types.SampledValue{},
		stopReason:map[int]core.Reason{}, stopCount:map[int]int{}, stopWh:map[int]int{},
		available:map[int]int{}, pulse:make(chan int,256),
	}
}
func (f *batteryFixtureCP) StatusNotification(connector int, _ core.ChargePointErrorCode, status core.ChargePointStatus, _ ...func(*core.StatusNotificationRequest)) (*core.StatusNotificationConfirmation,error) {
	f.mu.Lock()
	if status == core.ChargePointStatusAvailable { f.available[connector]++ }
	f.mu.Unlock()
	return &core.StatusNotificationConfirmation{},nil
}
func (f *batteryFixtureCP) Authorize(_ string,_ ...func(*core.AuthorizeRequest)) (*core.AuthorizeConfirmation,error) {
	return &core.AuthorizeConfirmation{IdTagInfo:types.NewIdTagInfo(types.AuthorizationStatusAccepted)},nil
}
func (f *batteryFixtureCP) StartTransaction(connector int,_ string,_ int,_ *types.DateTime,_ ...func(*core.StartTransactionRequest)) (*core.StartTransactionConfirmation,error) {
	f.mu.Lock()
	f.next[connector]++
	id:=connector*1000+f.next[connector]
	f.active[connector]=id
	f.mu.Unlock()
	return &core.StartTransactionConfirmation{TransactionId:id,IdTagInfo:types.NewIdTagInfo(types.AuthorizationStatusAccepted)},nil
}
func (f *batteryFixtureCP) MeterValues(connector int, values []types.MeterValue, props ...func(*core.MeterValuesRequest)) (*core.MeterValuesConfirmation,error) {
	request:=core.NewMeterValuesRequest(connector,values)
	for _,fn:=range props {fn(request)}
	f.mu.Lock()
	id:=f.active[connector]
	shouldFail:=f.failMeter
	if request.TransactionId==nil || *request.TransactionId!=id || len(values)==0 {
		f.mu.Unlock()
		return nil,errors.New("invalid meter transaction ID")
	}
	snapshot:=append([]types.SampledValue(nil),values[0].SampledValue...)
	if !shouldFail { f.samples[connector]=append(f.samples[connector],snapshot) }
	f.mu.Unlock()
	if shouldFail {return nil,errors.New("meter connection lost")}
	select {case f.pulse<-connector:default:}
	return &core.MeterValuesConfirmation{},nil
}
func (f *batteryFixtureCP) StopTransaction(meter int,_ *types.DateTime,tx int,props ...func(*core.StopTransactionRequest)) (*core.StopTransactionConfirmation,error){
	req:=core.NewStopTransactionRequest(meter,types.Now(),tx)
	for _,fn:=range props {fn(req)}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active[tx/1000]!=tx {return nil,errors.New("stop wrong CSMS ID")}
	f.stopCount[tx/1000]++
	f.stopWh[tx/1000]=meter
	f.stopReason[tx/1000]=req.Reason
	if f.failStop {return nil,errors.New("stop ACK lost")}
	return &core.StopTransactionConfirmation{},nil
}
func (f *batteryFixtureCP) sampleCopy(connector int) [][]types.SampledValue {
	f.mu.Lock()
	defer f.mu.Unlock()
	out:=make([][]types.SampledValue,len(f.samples[connector]))
	for i,x:=range f.samples[connector] {out[i]=append([]types.SampledValue(nil),x...)}
	return out
}
func (f *batteryFixtureCP) stopSnapshot(connector int) (int,int,core.Reason,int){
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopCount[connector],f.stopWh[connector],f.stopReason[connector],f.available[connector]
}

func batteryRemoteOptions(connector int, duration, interval time.Duration) ChargeOptions {
	return ChargeOptions{Connector:connector,RFID:"R4-LOCAL",PowerKW:7.2,Duration:duration,MeterInterval:interval}
}

func TestR4IndependentConcurrentBatteriesAndMeters(t *testing.T) {
	m,_:=NewSessionManager(context.Background(),2)
	defer m.Close()
	f:=newBatteryFixture()
	d,err:=NewRemoteStartDispatcher(m,f,RemoteChargeSettings{
		PowerKW:7.2,Duration:250*time.Millisecond,MeterInterval:50*time.Millisecond,
		BatteryKWh:0.02,BatterySOC:85,BatterySeed:19,
	})
	if err!=nil {t.Fatal(err)}
	if d.batteryFor(1)==d.batteryFor(2) || d.batteryFor(1)==nil {
		t.Fatal("connectors share battery state")
	}
	if d.batteryFor(1).snapshot().EnergyWh!=d.batteryFor(2).snapshot().EnergyWh {
		t.Fatal("unexpected initial SOC mismatch")
	}
	if remoteConnectorSeed(19,1)==remoteConnectorSeed(19,2) {
		t.Fatal("connector seed collision")
	}
	ctx1,_:=m.Reserve(1)
	ctx2,_:=m.Reserve(2)
	var wg sync.WaitGroup
	errs:=make(chan error,2)
	for _,job:=range []struct{ connector int; ctx context.Context }{{1,ctx1},{2,ctx2}}{
		wg.Add(1)
		go func(job struct{connector int;ctx context.Context}) {
			defer wg.Done()
			errs<-runRemoteCharge(job.ctx,f,m,batteryRemoteOptions(job.connector,250*time.Millisecond,50*time.Millisecond),d.batteryFor(job.connector))
		}(job)
	}
	wg.Wait()
	close(errs)
	for err:=range errs {if err!=nil {t.Fatal(err)}}
	for connector:=1;connector<=2;connector++ {
		samples:=f.sampleCopy(connector)
		if len(samples)<2 {t.Fatalf("connector %d emitted %d meter updates",connector,len(samples))}
		lastWh:=-1
		lastSOC:=0.0
		for _,batch:=range samples {
			if len(batch)!=3 || batch[0].Unit!=types.UnitOfMeasureWh ||
				batch[1].Unit!=types.UnitOfMeasureW || batch[2].Unit!=types.UnitOfMeasurePercent ||
				batch[0].Measurand!=types.MeasurandEnergyActiveImportRegister ||
				batch[1].Measurand!=types.MeasurandPowerActiveImport ||
				batch[2].Measurand!=types.MeasurandSoC {
				t.Fatalf("connector %d invalid Wh/W/SoC: %+v",connector,batch)
			}
			wh,e:=strconv.Atoi(batch[0].Value)
			if e!=nil || wh<lastWh {t.Fatalf("connector %d nonmonotonic Wh %s",connector,batch[0].Value)}
			watts,e:=strconv.Atoi(batch[1].Value)
			if e!=nil || watts<0 {t.Fatalf("connector %d invalid watts",connector)}
			soc,e:=strconv.ParseFloat(batch[2].Value,64)
			if e!=nil || soc<85 || soc>100 || soc<lastSOC {t.Fatalf("connector %d SOC %s",connector,batch[2].Value)}
			lastWh,lastSOC=wh,soc
		}
		n,stopWh,reason,available:=f.stopSnapshot(connector)
		if n!=1 || stopWh!=lastWh || reason!=core.ReasonLocal || available!=1 {
			t.Fatalf("connector %d stop count/meter/reason/status=%d/%d/%s/%d",connector,n,stopWh,reason,available)
		}
		requireSessionState(t,m,connector,SessionAvailable,0)
	}
	b1,b2:=d.batteryFor(1).snapshot(),d.batteryFor(2).snapshot()
	if b1.EnergyWh<=17 || b2.EnergyWh<=17 || b1.seed==b2.seed {
		t.Fatalf("missing independent energy advancement: %+v %+v",b1,b2)
	}
}

func TestR4ReproducibilityAndSequentialSessionMeterReset(t *testing.T) {
	settings:=RemoteChargeSettings{PowerKW:7.2,Duration:550*time.Millisecond,MeterInterval:50*time.Millisecond,BatteryKWh:0.05,BatterySOC:30,BatterySeed:7}
	var trace [2][][]types.SampledValue
	for replica:=0;replica<2;replica++ {
		m,_:=NewSessionManager(context.Background(),1)
		f:=newBatteryFixture()
		d,err:=NewRemoteStartDispatcher(m,f,settings)
		if err!=nil {t.Fatal(err)}
		before:=d.batteryFor(1).snapshot().EnergyWh
		for run:=0;run<2;run++ {
			ctx,err:=m.Reserve(1)
			if err!=nil {t.Fatal(err)}
			if err:=runRemoteCharge(ctx,f,m,batteryRemoteOptions(1,settings.Duration,settings.MeterInterval),d.batteryFor(1));err!=nil {t.Fatal(err)}
			requireSessionState(t,m,1,SessionAvailable,0)
			after:=d.batteryFor(1).snapshot().EnergyWh
			if after<=before {t.Fatal("battery did not persist charge across sessions")}
			before=after
		}
		trace[replica]=f.sampleCopy(1)
		if len(trace[replica])!=22 {t.Fatalf("expected two sessions with 11 samples each, got %d",len(trace[replica]))}
		// First energy reading of the second transaction is relative to that
		// transaction, not to the battery's accumulated state.
		firstSecond,err:=strconv.Atoi(trace[replica][11][0].Value)
		if err!=nil || firstSecond!=0 {t.Fatalf("second transaction meter not reset: %d %v",firstSecond,err)}
		m.Close()
	}
	if !reflect.DeepEqual(trace[0],trace[1]) {t.Fatal("replay with same seed/connector diverged")}
}

func TestR4NaturalFullAndRejectAlreadyFull(t *testing.T) {
	m,_:=NewSessionManager(context.Background(),1)
	defer m.Close()
	f:=newBatteryFixture()
	d,err:=NewRemoteStartDispatcher(m,f,RemoteChargeSettings{
		PowerKW:7.2,Duration:time.Second,MeterInterval:10*time.Millisecond,
		BatteryKWh:0.00001,BatterySOC:95,BatterySeed:91,
	})
	if err!=nil {t.Fatal(err)}
	ctx,_:=m.Reserve(1)
	start:=time.Now()
	if err:=runRemoteCharge(ctx,f,m,batteryRemoteOptions(1,time.Second,10*time.Millisecond),d.batteryFor(1));err!=nil {t.Fatal(err)}
	if time.Since(start)>500*time.Millisecond {t.Fatal("natural full stop ran until timeout")}
	if !d.batteryFor(1).full() || d.batteryFor(1).snapshot().SOC()!=100 {
		t.Fatal("battery did not reach exact 100%")
	}
	n,_,reason,available:=f.stopSnapshot(1)
	if n!=1 || reason!=core.ReasonLocal || available!=1 {t.Fatalf("natural full did not stop once: %d %s %d",n,reason,available)}
	if err:=d.Enable();err!=nil {t.Fatal(err)}
	defer d.Close()
	if d.TryStart(core.NewRemoteStartTransactionRequest("R4-USER")) {
		t.Fatal("accepted a remote start for a full battery")
	}
}

func TestR4RemoteStopPreservesBatteryAndTransaction(t *testing.T) {
	m,_:=NewSessionManager(context.Background(),1)
	defer m.Close()
	f:=newBatteryFixture()
	d,err:=NewRemoteStartDispatcher(m,f,RemoteChargeSettings{
		PowerKW:7.2,Duration:2*time.Second,MeterInterval:20*time.Millisecond,
		BatteryKWh:0.01,BatterySOC:40,BatterySeed:13,
	})
	if err!=nil {t.Fatal(err)}
	ctx,_:=m.Reserve(1)
	result:=make(chan error,1)
	go func(){result<-runRemoteCharge(ctx,f,m,batteryRemoteOptions(1,2*time.Second,20*time.Millisecond),d.batteryFor(1))}()
	select {
	case connector:=<-f.pulse: if connector!=1 {t.Fatal(connector)}
	case <-time.After(2*time.Second):t.Fatal("missing initial battery MeterValues")
	}
	if _,err:=m.RequestRemoteStop(1001);err!=nil {t.Fatal(err)}
	if _,err:=m.RequestRemoteStop(1001);err!=nil {t.Fatal("non-idempotent stop",err)}
	select {
	case err:=<-result:if err!=nil {t.Fatal(err)}
	case <-time.After(3*time.Second):t.Fatal("remote session did not stop")
	}
	n,_,reason,available:=f.stopSnapshot(1)
	if n!=1 || reason!=core.ReasonRemote || available!=1 {t.Fatalf("bad RemoteStop: %d %s %d",n,reason,available)}
	if d.batteryFor(1).full() {t.Fatal("remote stop unexpectedly filled battery")}
	if _,err:=m.RequestRemoteStop(1001);err==nil {t.Fatal("stale transaction accepted")}
}

func TestR4FailedMeterOrStopNeverFakesClosure(t *testing.T) {
	for _,tc:=range []struct{label string;meter bool;stop bool}{
		{"meter unconfirmed",true,false},{"stop unconfirmed",false,true},
	}{
		t.Run(tc.label,func(t *testing.T){
			m,_:=NewSessionManager(context.Background(),1)
			defer m.Close()
			f:=newBatteryFixture()
			f.failMeter=tc.meter
			f.failStop=tc.stop
			d,_:=NewRemoteStartDispatcher(m,f,RemoteChargeSettings{
				PowerKW:7.2,Duration:20*time.Millisecond,MeterInterval:10*time.Millisecond,
				BatteryKWh:0.01,BatterySOC:50,BatterySeed:4,
			})
			ctx,_:=m.Reserve(1)
			if err:=runRemoteCharge(ctx,f,m,batteryRemoteOptions(1,20*time.Millisecond,10*time.Millisecond),d.batteryFor(1));err==nil {
				t.Fatal("connection failure reported as successful transaction")
			}
			requireSessionState(t,m,1,SessionStopping,1001)
			n,_,_,available:=f.stopSnapshot(1)
			if tc.meter && n!=0 {t.Fatalf("sent StopTransaction after failed MeterValues: %d",n)}
			if tc.stop && n!=1 {t.Fatalf("expected one unconfirmed stop attempt: %d",n)}
			if available!=0 {t.Fatal("fabricated Available after unconfirmed closure")}
			if _,err:=m.Reserve(1);err==nil {t.Fatal("reused unresolved connector")}
			if d.batteryFor(1).snapshot().EnergyWh<=5 {t.Fatal("lost simulated battery energy after error")}
		})
	}
}

func TestR4InvalidAndIgnoredBatteryConfiguration(t *testing.T) {
	for _,o:=range []RemoteChargeSettings{
		{BatteryKWh:-1},
		{BatteryKWh:math.NaN()},
		{BatteryKWh:math.Inf(1)},
		{BatteryKWh:1e10},
		{BatterySOC:30},
		{BatterySeed:5},
		{BatteryKWh:1,BatterySOC:-1},
		{BatteryKWh:1,BatterySOC:101},
		{BatteryKWh:1,BatterySOC:math.NaN()},
		// This can be low total Wh for a nanosecond interval, but the
		// instantaneous OCPP power would overflow the 32-bit W measurand.
		{PowerKW:1e10,Duration:time.Nanosecond,MeterInterval:time.Nanosecond,BatteryKWh:1},
	} {
		if _,err:=o.normalized();err==nil {t.Fatalf("accepted invalid or ignored battery settings: %+v",o)}
	}
	settings,err:=(RemoteChargeSettings{BatteryKWh:1,BatterySOC:0,BatterySeed:0}).normalized()
	if err!=nil || settings.BatteryKWh!=1 {t.Fatal("rejected valid zero seed/SOC",settings,err)}
	if remoteConnectorSeed(3,1)!=remoteConnectorSeed(3,1) || remoteConnectorSeed(3,1)==remoteConnectorSeed(3,2) {
		t.Fatal("connector seeds are not independent and repeatable")
	}
}
