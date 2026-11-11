package spike

import (
 "bufio"
 "context"
 "errors"
 "fmt"
 "os"
 "os/exec"
 "strings"
 "sync"
 "testing"
 "time"

 "github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
)

func r6c2Open(t *testing.T) (*SQLiteRecoveryStore,string){
 t.Helper()
 root:=r6c1PrivateRoot(t)
 s,err:=OpenSQLiteRecoveryStore(root,r6c1Identity())
 if err!=nil{t.Fatal(err)}
 return s,root
}
func r6c2Attempt(t *testing.T,s *SQLiteRecoveryStore,phase string)RecoveryAttempt{
 t.Helper()
 attempts,err:=s.Attempts(context.Background())
 if err!=nil{t.Fatal(err)}
 if len(attempts)!=1||attempts[0].Phase!=phase{t.Fatalf("want %s, attempts=%+v",phase,attempts)}
 return attempts[0]
}
func TestR6C2DurableIntentBeforeStopAndRelease(t *testing.T){
 s,root:=r6c2Open(t)
 ctx:=context.Background()
 m,err:=newSessionManagerWithRecovery(ctx,2,s)
 if err!=nil{t.Fatal(err)}
 session,err:=m.Reserve(1);if err!=nil||session==nil{t.Fatal(err)}
 r6c2Attempt(t,s,"Reserved")
 b,err:=NewBattery(40,30);if err!=nil{t.Fatal(err)}
 b.SetSeed(0xffffffffffffffff)
 if err:=m.InitializeBattery(1,RecoveryBattery{CapacityWh:b.CapacityWh,EnergyWh:b.EnergyWh,Seed:b.seed});err!=nil{t.Fatal(err)}
 if got:=r6c2Attempt(t,s,"Reserved");got.Battery.Seed!=b.seed||got.Battery.EnergyWh!=b.EnergyWh{t.Fatal(got)}
 if err:=m.BeginStart(1);err!=nil{t.Fatal(err)}
 if a:=r6c2Attempt(t,s,"StartIntent");a.TransactionID!=0{t.Fatal("fabricated CSMS ID")}
 if err:=m.Start(1,19001);err!=nil{t.Fatal(err)}
 r6c2Attempt(t,s,"ChargingKnown")
 b.AdvanceSeeded(7.2,time.Second)
 if err:=m.RecordMeterIntent(1,19001,2,7200,RecoveryBattery{CapacityWh:b.CapacityWh,EnergyWh:b.EnergyWh,Seed:b.seed,Drift:b.drift});err!=nil{t.Fatal(err)}
 got:=r6c2Attempt(t,s,"MeterIntent")
 if got.MeterWh!=2||got.Battery.Seed!=b.seed||got.Battery.EnergyWh!=b.EnergyWh{t.Fatalf("seed/Wh not durably recorded: %+v",got)}
 if err:=m.RecordMeterConfirmation(1,19001);err!=nil{t.Fatal(err)}
 r6c2Attempt(t,s,"MeterAcked")
 if _,err:=m.RequestRemoteStop(19001);err!=nil{t.Fatal(err)}
 r6c2Attempt(t,s,"StopRequested")
 reason,err:=m.BeginStopReasonWithMeter(1,19001,2)
 if err!=nil||reason!=core.ReasonRemote{t.Fatal(reason,err)}
 got=r6c2Attempt(t,s,"StopIntent")
 if got.StopReason!="Remote"||got.TransactionID!=19001||got.MeterWh!=2{t.Fatalf("stop data lost: %+v",got)}
 if _,err:=m.BeginStopReasonWithMeter(1,19001,2);err==nil{t.Fatal("duplicate StopTransaction admitted")}
 if err:=m.RecordStopConfirmation(1,19001);err!=nil{t.Fatal(err)}
 r6c2Attempt(t,s,"StopAcked")
 if err:=m.Finish(1,19001);err!=nil{t.Fatal(err)}
 r6c2Attempt(t,s,"Closed")
 m.Close()
 if err:=s.Close();err!=nil{t.Fatal(err)}
 reopened,err:=OpenSQLiteRecoveryStore(root,r6c1Identity());if err!=nil{t.Fatal(err)}
 defer reopened.Close()
 after:=r6c2Attempt(t,reopened,"Closed")
 if after.TransactionID!=19001||after.MeterWh!=2||after.Battery.Seed!=b.seed{t.Fatalf("lost durable CSMS receipt: %+v",after)}
 if !errors.Is(reopened.BeforeAvailable(ctx),ErrRecoveryQuarantined){t.Fatal("CSMS authority bypass")}
}

func TestR6C2UnknownStartAndStopAckSurviveRestart(t *testing.T){
 for _,mode:=range []string{"start-unknown","stop-unknown"}{
  t.Run(mode,func(t *testing.T){
   s,root:=r6c2Open(t)
   m,_:=newSessionManagerWithRecovery(context.Background(),2,s)
   _,err:=m.Reserve(1);if err!=nil{t.Fatal(err)}
   if err:=m.BeginStart(1);err!=nil{t.Fatal(err)}
   expect:="StartIntent"
   if mode=="stop-unknown"{
    if err:=m.Start(1,6201);err!=nil{t.Fatal(err)}
    if err:=m.RecordMeterIntent(1,6201,3,7000,RecoveryBattery{});err!=nil{t.Fatal(err)}
    if err:=m.RecordMeterConfirmation(1,6201);err!=nil{t.Fatal(err)}
    if _,err:=m.BeginStopReasonWithMeter(1,6201,3);err!=nil{t.Fatal(err)}
    expect="StopIntent"
   }
   m.Close()
   _=s.Close()
   next,err:=OpenSQLiteRecoveryStore(root,r6c1Identity());if err!=nil{t.Fatal(err)}
   defer next.Close()
   a:=r6c2Attempt(t,next,expect)
   if mode=="start-unknown"&&a.TransactionID!=0||mode=="stop-unknown"&&(a.TransactionID!=6201||a.MeterWh!=3){
    t.Fatalf("uncertain receipt replaced on restart: %+v",a)
   }
   m2,_:=newSessionManagerWithRecovery(context.Background(),2,next)
   defer m2.Close()
   if _,err:=m2.Reserve(1);err==nil{t.Fatal("reused quarantined connector")}
   if !errors.Is(next.BeforeAvailable(context.Background()),ErrRecoveryQuarantined){t.Fatal("startup unlocked")}
  })
 }
}

func TestR6C2FailBeforeStartAndMeterNoNetworkSend(t *testing.T){
 s,root:=r6c2Open(t)
 m,_:=newSessionManagerWithRecovery(context.Background(),2,s)
 _,err:=m.Reserve(1);if err!=nil{t.Fatal(err)}
 s.beforeCommit=func()error{return errors.New("EIO before StartIntent")}
 if err:=m.BeginStart(1);err==nil{t.Fatal("start could be sent without durable intent")}
 m.Close();_ = s.Close()
 reopened,err:=OpenSQLiteRecoveryStore(root,r6c1Identity());if err!=nil{t.Fatal(err)}
 defer reopened.Close()
 r6c2Attempt(t,reopened,"Reserved")
 if !errors.Is(reopened.BeforeAvailable(context.Background()),ErrRecoveryQuarantined){t.Fatal("failure unlocked startup")}
}

func TestR6C2ConcurrentSingleOwnerAndDuplicateID(t *testing.T){
 s,_:=r6c2Open(t);defer s.Close()
 m,_:=newSessionManagerWithRecovery(context.Background(),2,s);defer m.Close()
 var wg sync.WaitGroup
 results:=make(chan bool,32)
 for i:=0;i<32;i++{wg.Add(1);go func(){defer wg.Done();_,err:=m.Reserve(1);results<-err==nil}()}
 wg.Wait();close(results)
 n:=0;for ok:=range results{if ok{n++}}
 if n!=1{t.Fatalf("expected one durable owner; got %d",n)}
 if err:=m.Start(1,7001);err!=nil{t.Fatal(err)}
 _,err:=m.Reserve(2);if err!=nil{t.Fatal(err)}
 if err:=m.Start(2,7001);err==nil{t.Fatal("same CSMS ID assigned to two connectors")}
 attempts,err:=s.Attempts(context.Background());if err!=nil{t.Fatal(err)}
 if len(attempts)!=2{t.Fatal(attempts)}
 if attempts[0].TransactionID!=7001||attempts[1].TransactionID!=0{t.Fatal("duplicate ID corrupted durable ledger",attempts)}
}

func TestR6C2AbruptSIGKILLPersistedStopIntent(t *testing.T){
 if os.Getenv("R6C2_KILL_CHILD")=="1"{
  s,err:=OpenSQLiteRecoveryStore(os.Getenv("R6C2_KILL_ROOT"),r6c1Identity())
  if err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(31)}
  m,_:=newSessionManagerWithRecovery(context.Background(),2,s)
  _,err=m.Reserve(1);if err!=nil{os.Exit(32)}
  if err=m.BeginStart(1);err!=nil{os.Exit(33)}
  if err=m.Start(1,7401);err!=nil{os.Exit(34)}
  if err=m.RecordMeterIntent(1,7401,7,7000,RecoveryBattery{});err!=nil{os.Exit(35)}
  if err=m.RecordMeterConfirmation(1,7401);err!=nil{os.Exit(36)}
  if _,err=m.BeginStopReasonWithMeter(1,7401,7);err!=nil{os.Exit(37)}
  fmt.Println("DURABLE_STOP_INTENT_READY")
  for{time.Sleep(time.Hour)}
 }
 root:=r6c1PrivateRoot(t)
 ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel()
 cmd:=exec.CommandContext(ctx,os.Args[0],"-test.run=^TestR6C2AbruptSIGKILLPersistedStopIntent$")
 cmd.Env=append(os.Environ(),"R6C2_KILL_CHILD=1","R6C2_KILL_ROOT="+root)
 stdout,err:=cmd.StdoutPipe();if err!=nil{t.Fatal(err)}
 if err:=cmd.Start();err!=nil{t.Fatal(err)}
 ready:=make(chan string,1)
 go func(){line,_:=bufio.NewReader(stdout).ReadString('\n');ready<-line}()
 select{
 case line:=<-ready:
  if !strings.Contains(line,"DURABLE_STOP_INTENT_READY"){_ = cmd.Process.Kill();t.Fatalf("child never persisted stop: %q",line)}
 case <-ctx.Done():
  _=cmd.Process.Kill();t.Fatal("child failed to persist before deadline")
 }
 if err:=cmd.Process.Kill();err!=nil{t.Fatal(err)}
 _=cmd.Wait()
 reopened,err:=OpenSQLiteRecoveryStore(root,r6c1Identity());if err!=nil{t.Fatal(err)}
 defer reopened.Close()
 a:=r6c2Attempt(t,reopened,"StopIntent")
 if a.TransactionID!=7401||a.MeterWh!=7{t.Fatalf("SIGKILL lost intent: %+v",a)}
 if !errors.Is(reopened.BeforeAvailable(context.Background()),ErrRecoveryQuarantined){t.Fatal("SIGKILL cleared quarantine")}
}

type r6c2CheckingCP struct {
 *remoteTestCP
 store *SQLiteRecoveryStore
 errors chan error
}
func(c *r6c2CheckingCP) verify(phase string){
 attempts,err:=c.store.Attempts(context.Background())
 if err!=nil||len(attempts)!=1||attempts[0].Phase!=phase{
  select{case c.errors<-fmt.Errorf("OCPP send before durable %s: %+v %v",phase,attempts,err):default:}
 }
}
func(c *r6c2CheckingCP) StartTransaction(connector int,tag string,meter int,at *types.DateTime,opts ...func(*core.StartTransactionRequest))(*core.StartTransactionConfirmation,error){
 c.verify("StartIntent")
 return c.remoteTestCP.StartTransaction(connector,tag,meter,at,opts...)
}
func(c *r6c2CheckingCP) MeterValues(connector int,values []types.MeterValue,opts ...func(*core.MeterValuesRequest))(*core.MeterValuesConfirmation,error){
 c.verify("MeterIntent")
 return c.remoteTestCP.MeterValues(connector,values,opts...)
}
func(c *r6c2CheckingCP) StopTransaction(wh int,at *types.DateTime,txID int,opts ...func(*core.StopTransactionRequest))(*core.StopTransactionConfirmation,error){
 c.verify("StopIntent")
 return c.remoteTestCP.StopTransaction(wh,at,txID,opts...)
}
func TestR6C2DispatcherCommitBeforeEveryOCPPCommand(t *testing.T){
 s,_:=r6c2Open(t);defer s.Close()
 m,_:=newSessionManagerWithRecovery(context.Background(),2,s);defer m.Close()
 cp:=&r6c2CheckingCP{remoteTestCP:remoteAcceptedFake(),store:s,errors:make(chan error,10)}
 d,err:=NewRemoteStartDispatcher(m,cp,RemoteChargeSettings{
  PowerKW:7.2,Duration:120*time.Millisecond,MeterInterval:30*time.Millisecond,
  BatteryKWh:40,BatterySOC:30,BatterySeed:0xffffffffffffffff,
 })
 if err!=nil{t.Fatal(err)}
 if err:=d.Enable();err!=nil{t.Fatal(err)}
 req:=core.NewRemoteStartTransactionRequest("R6C2-RFID")
 if !d.TryStart(req){t.Fatal("admission failed despite valid private test journal")}
 deadline:=time.Now().Add(3*time.Second)
 for {
  attempts,err:=s.Attempts(context.Background());if err!=nil{t.Fatal(err)}
  if len(attempts)==1&&attempts[0].Phase=="Closed" {
   if attempts[0].TransactionID!=91||attempts[0].Battery.CapacityWh!=40000||attempts[0].Battery.Seed==0 {
    t.Fatal("wrong CSMS ID or battery not persisted",attempts)
   }
   break
  }
  if time.Now().After(deadline){t.Fatalf("worker failed durable close: attempts=%+v errors=%s",attempts,cp.history())}
  time.Sleep(5*time.Millisecond)
 }
 d.Close()
 select{case <-d.Done():case <-time.After(2*time.Second):t.Fatal("dispatcher not bounded")}
 select{case e:=<-cp.errors:t.Fatal(e);default:}
 if !strings.Contains(cp.history(),"StartTransaction")||!strings.Contains(cp.history(),"StopTransaction"){
  t.Fatal("OCPP fixture never exercised command sends")
 }
}
