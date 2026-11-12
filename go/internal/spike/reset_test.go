package spike

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
)

func resetDispatcherForTest(t *testing.T, connectors int) (*SessionManager, *RemoteStartDispatcher) {
	t.Helper()
	m, err := NewSessionManager(context.Background(), connectors)
	if err != nil { t.Fatal(err) }
	d, err := NewRemoteStartDispatcher(m, remoteAcceptedFake(), RemoteChargeSettings{})
	if err != nil { t.Fatal(err) }
	if err := d.Enable(); err != nil { t.Fatal(err) }
	t.Cleanup(func() {
		d.Close()
		select {
		case <-d.Done():
		case <-time.After(2*time.Second):t.Error("dispatcher did not exit")
		}
	})
	return m, d
}

func TestR5ResetRejectInvalidAndDuplicate(t *testing.T) {
	m,d:=resetDispatcherForTest(t,1)
	for _,r:=range []*core.ResetRequest{nil,{},core.NewResetRequest(core.ResetType("Invalid"))}{
		if d.TryReset(r) {t.Fatalf("accepted invalid reset %+v",r)}
	}
	if !d.TryReset(core.NewResetRequest(core.ResetTypeSoft)){t.Fatal("soft reset rejected")}
	select{
	case mode:=<-d.ResetEvents():
		if mode!=core.ResetTypeSoft {t.Fatalf("wrong mode %q",mode)}
	default:t.Fatal("no reset signal")
	}
	if d.TryReset(core.NewResetRequest(core.ResetTypeSoft)) || d.TryReset(core.NewResetRequest(core.ResetTypeHard)){
		t.Fatal("accepted duplicate or conflicting reset")
	}
	if d.TryStart(core.NewRemoteStartTransactionRequest("R5-TEST")){
		t.Fatal("reset accepted a new remote start")
	}
	if d.TryStop(&core.RemoteStopTransactionRequest{TransactionId:1001}){
		t.Fatal("reset accepted a remote stop command")
	}
	if mode:=m.ResetMode();mode!=core.ResetTypeSoft {t.Fatal(mode)}
}

func TestR5SoftResetRetainsIDUntilVerifiedStop(t *testing.T) {
	m,d:=resetDispatcherForTest(t,2)
	one,_:=m.Reserve(1)
	two,_:=m.Reserve(2)
	if err:=m.Start(1,601);err!=nil {t.Fatal(err)}
	if err:=m.Start(2,602);err!=nil {t.Fatal(err)}
	if _,err:=m.RequestRemoteStop(601);err!=nil {t.Fatal(err)}
	if !d.TryReset(core.NewResetRequest(core.ResetTypeSoft)) {t.Fatal("soft reset failed")}
	select{case <-one.Done():default:t.Fatal("connector one not cancelled")}
	select{case <-two.Done():default:t.Fatal("connector two not cancelled")}
	if _,err:=m.Reserve(1);err==nil {t.Fatal("accepted after reset")}
	first,err:=m.BeginStopReason(1,601)
	if err!=nil||first!=core.ReasonRemote {t.Fatalf("existing RemoteStop overwritten: %v %v",first,err)}
	second,err:=m.BeginStopReason(2,602)
	if err!=nil||second!=core.ReasonSoftReset {t.Fatalf("soft reset reason mismatch: %v %v",second,err)}
	if got:=m.Unresolved();len(got)!=2||got[0].TransactionID!=601||got[1].TransactionID!=602 {
		t.Fatalf("unresolved identities lost: %+v",got)
	}
	if err:=m.Finish(1,601);err!=nil {t.Fatal(err)}
	if len(m.Unresolved())!=1 {t.Fatal("unfinished second tx disappeared")}
	if err:=m.Finish(2,602);err!=nil {t.Fatal(err)}
	if len(m.Unresolved())!=0 {t.Fatal("verified finished tx still unresolved")}
}

func TestR5HardResetRetainsLateStartAndForbidsStop(t *testing.T) {
	m,d:=resetDispatcherForTest(t,2)
	one,_:=m.Reserve(1)
	two,_:=m.Reserve(2)
	if err:=m.BeginStart(1);err!=nil {t.Fatal(err)}
	if err:=m.Start(2,702);err!=nil {t.Fatal(err)}
	if !d.TryReset(core.NewResetRequest(core.ResetTypeHard)) {t.Fatal("hard reset failed")}
	select{case <-one.Done():default:t.Fatal("starting context not cancelled")}
	select{case <-two.Done():default:t.Fatal("active context not cancelled")}
	if !m.IsHardReset() {t.Fatal("hard mode lost")}
	if _,err:=m.BeginStopReason(2,702);err==nil {t.Fatal("hard reset permitted StopTransaction")}
	if got:=m.Unresolved();len(got)!=2||got[0].State!=SessionStarting||got[1].TransactionID!=702{
		t.Fatalf("pending/known IDs lost on Hard: %+v",got)
	}
	// The CSMS may return a real ID after Hard was accepted.
	if err:=m.Start(1,701);err!=nil {t.Fatal("late real CSMS ID lost: ",err)}
	if got:=m.Unresolved();len(got)!=2||got[0].TransactionID!=701||got[0].State!=SessionStopping {
		t.Fatalf("late accepted ID not retained: %+v",got)
	}
	if _,err:=m.RequestRemoteStop(701);err==nil {t.Fatal("remote stop accepted after Hard")}
}

func TestR5SoftResetWorkerStopsAndPreservesRemoteReason(t *testing.T) {
	for _,tc:=range []struct{name string;preRemote bool;failStop bool;reason core.Reason;unresolved bool}{
		{"soft",false,false,core.ReasonSoftReset,false},
		{"remote before soft",true,false,core.ReasonRemote,false},
		{"unconfirmed soft",false,true,core.ReasonSoftReset,true},
	} {
		t.Run(tc.name,func(t *testing.T){
			cp:=remoteAcceptedFake()
			if tc.failStop {cp.stopErr=errors.New("lost StopTransaction response")}
			m,err:=NewSessionManager(context.Background(),1)
			if err!=nil {t.Fatal(err)}
			defer m.Close()
			d,err:=NewRemoteStartDispatcher(m,cp,RemoteChargeSettings{Duration:5*time.Second,MeterInterval:10*time.Millisecond})
			if err!=nil {t.Fatal(err)}
			if err:=d.Enable();err!=nil {t.Fatal(err)}
			defer d.Close()
			ctx,_:=m.Reserve(1)
			result:=make(chan error,1)
			opts:=remoteTestOptions()
			opts.Duration=5*time.Second
			go func(){result<-RunRemoteCharge(ctx,cp,m,opts)}()
			deadline:=time.Now().Add(2*time.Second)
			for {
				snap,_:=m.Snapshot(1)
				if snap.State==SessionCharging {break}
				if time.Now().After(deadline){t.Fatal("never started test transaction")}
				time.Sleep(time.Millisecond)
			}
			if tc.preRemote {
				if _,err:=m.RequestRemoteStop(91);err!=nil {t.Fatal(err)}
			}
			if !d.TryReset(core.NewResetRequest(core.ResetTypeSoft)){t.Fatal("reset rejected")}
			select {
			case err:=<-result:
				if tc.unresolved && err==nil {t.Fatal("failed stop reported success")}
				if !tc.unresolved && err!=nil {t.Fatalf("soft reset did not close: %v",err)}
			case <-time.After(2*time.Second):t.Fatal("soft reset worker did not exit")
			}
			cp.mu.Lock()
			reason,count:=cp.stopReason,cp.stopCount
			cp.mu.Unlock()
			if reason!=tc.reason||count!=1 {t.Fatalf("wrong reason/count: %s/%d",reason,count)}
			unresolved:=m.Unresolved()
			if tc.unresolved {
				if len(unresolved)!=1||unresolved[0].TransactionID!=91 {t.Fatalf("unconfirmed stop cleared ID: %+v",unresolved)}
				if strings.Contains(cp.history(),"Available"){t.Fatal("fabricated Available after lost ACK")}
			}else if len(unresolved)!=0{
				t.Fatalf("confirmed StopTransaction left unresolved: %+v",unresolved)
			}
		})
	}
}

func TestR5HardResetWorkerNeverFakesStop(t *testing.T) {
	cp:=remoteAcceptedFake()
	m,_:=NewSessionManager(context.Background(),1)
	defer m.Close()
	d,err:=NewRemoteStartDispatcher(m,cp,RemoteChargeSettings{})
	if err!=nil {t.Fatal(err)}
	if err:=d.Enable();err!=nil {t.Fatal(err)}
	defer d.Close()
	ctx,_:=m.Reserve(1)
	result:=make(chan error,1)
	opts:=remoteTestOptions()
	opts.Duration=5*time.Second
	go func(){result<-RunRemoteCharge(ctx,cp,m,opts)}()
	deadline:=time.Now().Add(2*time.Second)
	for {
		snap,_:=m.Snapshot(1)
		if snap.State==SessionCharging {break}
		if time.Now().After(deadline){t.Fatal("test never reached Charging")}
		time.Sleep(time.Millisecond)
	}
	if !d.TryReset(core.NewResetRequest(core.ResetTypeHard)){t.Fatal("hard reset rejected")}
	select{
	case err:=<-result:
		if err==nil||!strings.Contains(err.Error(),"hard reset"){t.Fatalf("hard reset was hidden: %v",err)}
	case <-time.After(2*time.Second):t.Fatal("hard reset worker did not exit")
	}
	if strings.Contains(cp.history(),"StopTransaction")||strings.Contains(cp.history(),"Available"){
		t.Fatalf("fabricated completion on Hard: %s",cp.history())
	}
	if got:=m.Unresolved();len(got)!=1||got[0].TransactionID!=91 {t.Fatalf("hard lost CSMS ID: %+v",got)}
}

func TestR5HardResetDuringLateStartConfirmation(t *testing.T) {
	gate:=make(chan struct{})
	cp:=remoteAcceptedFake()
	cp.startGate=gate
	m,_:=NewSessionManager(context.Background(),1)
	defer m.Close()
	d,err:=NewRemoteStartDispatcher(m,cp,RemoteChargeSettings{})
	if err!=nil{t.Fatal(err)}
	if err:=d.Enable();err!=nil{t.Fatal(err)}
	defer d.Close()
	ctx,_:=m.Reserve(1)
	result:=make(chan error,1)
	go func(){result<-RunRemoteCharge(ctx,cp,m,remoteTestOptions())}()
	deadline:=time.Now().Add(2*time.Second)
	for{
		snap,_:=m.Snapshot(1)
		if snap.State==SessionStarting {break}
		if time.Now().After(deadline){t.Fatal("never in Starting")}
		time.Sleep(time.Millisecond)
	}
	if !d.TryReset(core.NewResetRequest(core.ResetTypeHard)){t.Fatal("Hard rejected")}
	close(gate)
	select{
	case err:=<-result:
		if err==nil||!strings.Contains(err.Error(),"hard reset"){t.Fatalf("late response was treated as success: %v",err)}
	case <-time.After(2*time.Second):t.Fatal("in-flight StartTransaction not resolved")
	}
	if got:=m.Unresolved();len(got)!=1||got[0].TransactionID!=91{
		t.Fatalf("late real CSMS ID lost: %+v",got)
	}
	if strings.Contains(cp.history(),"StopTransaction"){t.Fatal("Hard invented a StopTransaction")}
}

func TestR5ConcurrentRemoteStartStopResetNoDeadlock(t *testing.T) {
	for iteration:=0;iteration<40;iteration++{
		m,_:=NewSessionManager(context.Background(),1)
		cp:=remoteAcceptedFake()
		d,err:=NewRemoteStartDispatcher(m,cp,RemoteChargeSettings{Duration:100*time.Millisecond,MeterInterval:10*time.Millisecond})
		if err!=nil{t.Fatal(err)}
		if err:=d.Enable();err!=nil{t.Fatal(err)}
		// Reserve/Start directly to provide one recognized ID while
		// all three command handlers contend on d.mu and m.mu.
		_,err=m.Reserve(1)
		if err!=nil{t.Fatal(err)}
		if err=m.Start(1,91);err!=nil{t.Fatal(err)}
		var wg sync.WaitGroup
		for _,op:=range []func(){
			func(){d.TryStop(&core.RemoteStopTransactionRequest{TransactionId:91})},
			func(){d.TryReset(core.NewResetRequest(core.ResetTypeSoft))},
			func(){d.TryStart(core.NewRemoteStartTransactionRequest("R5"))},
		}{
			wg.Add(1)
			go func(run func()){defer wg.Done();run()}(op)
		}
		done:=make(chan struct{})
		go func(){wg.Wait();close(done)}()
		select{
		case <-done:
		case <-time.After(time.Second):t.Fatal("deadlock under concurrent reset")
		}
		if got:=m.Unresolved();len(got)!=1||got[0].TransactionID!=91 {t.Fatalf("race lost ID: %+v",got)}
		d.Close()
		select{case <-d.Done():case <-time.After(time.Second):t.Fatal("dispatcher not stopped")}
	}
}
