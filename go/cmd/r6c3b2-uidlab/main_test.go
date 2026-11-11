package main

import (
 "encoding/json"
 "errors"
 "fmt"
 "os"
 "os/exec"
 "path/filepath"
 "sync"
 "syscall"
 "testing"

 "github.com/lorenzodonini/ocpp-go/ocpp"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
 "github.com/lorenzodonini/ocpp-go/ocppj"
 "github.com/lorenzodonini/ocpp-go/ws"
)
func newJournal(t *testing.T) string {
 t.Helper()
 dir:=filepath.Join(t.TempDir(),"journal")
 if err:=os.Mkdir(dir,0700);err!=nil{t.Fatal(err)}
 return dir
}
type fakeQueue struct {
 ocppj.ClientDispatcher
 mu sync.Mutex
 calls int
 fail bool
 dir string
}
func (*fakeQueue) SetNetworkClient(ws.WsClient) {}
func (*fakeQueue) SetPendingRequestState(ocppj.ClientState) {}
func (*fakeQueue) IsRunning() bool {return true}
func(q *fakeQueue) SendRequest(b ocppj.RequestBundle) error {
 if b.Call==nil{return fmt.Errorf("no call")}
 raw,err:=os.ReadFile(filepath.Join(q.dir,bindingName("uid",b.Call.UniqueId)))
 if err!=nil{return err}
 var i intent
 if json.Unmarshal(raw,&i)!=nil||!validIntent(i)||i.UID!=b.Call.UniqueId{return errors.New("not durable")}
 if _,err=os.Stat(filepath.Join(q.dir,bindingName("attempt",i.AttemptID)));err!=nil{return err}
 q.mu.Lock();q.calls++;q.mu.Unlock()
 if q.fail{return errors.New("simulated post-enqueue ACK uncertainty")}
 return nil
}
func(q *fakeQueue) count()int{q.mu.Lock();defer q.mu.Unlock();return q.calls}
func request()*core.StartTransactionRequest{return core.NewStartTransactionRequest(1,"lab-rfid",0,types.Now())}
func frame(t *testing.T,req *core.StartTransactionRequest,uid string)ocppj.RequestBundle{
 t.Helper()
 call:=&ocppj.Call{MessageTypeId:ocppj.CALL,UniqueId:uid,Action:core.StartTransactionFeatureName,Payload:req}
 data,err:=call.MarshalJSON();if err!=nil{t.Fatal(err)}
 return ocppj.RequestBundle{Call:call,Data:data}
}
func TestLAB6RealOCPPClientGeneratesUIDBeforeDelegate(t *testing.T){
 dir:=newJournal(t);sink:=&fakeQueue{dir:dir};d:=newUIDDispatcher(dir,sink)
 c:=ocppj.NewClient(labCP,ws.NewClient(),d,nil,core.Profile);c.SetDialect(ocpp.V16)
 req:=request()
 if err:=d.bind(req,newIntent("real-client","pending-bind"));err!=nil{t.Fatal(err)}
 if err:=c.SendRequest(req);err!=nil{t.Fatal(err)}
 if sink.count()!=1{t.Fatalf("delegated %d times",sink.count())}
 items,_:=os.ReadDir(dir);if len(items)!=2{t.Fatalf("journal count %d",len(items))}
}
func TestLAB6FailClosedOnUnboundMalformedAndMutation(t *testing.T){
 for _,kind:=range []string{"unbound","malformed","mutated","uid","wire"}{
  t.Run(kind,func(t *testing.T){
   dir:=newJournal(t);sink:=&fakeQueue{dir:dir};d:=newUIDDispatcher(dir,sink)
   req:=request()
   if kind!="unbound"{if err:=d.bind(req,newIntent("a","pending-bind"));err!=nil{t.Fatal(err)}}
   b:=frame(t,req,"id-1")
   switch kind{
   case "malformed": b.Data=[]byte("bad")
   case "mutated": req.MeterStart=7
   case "uid": b.Call.UniqueId="different"
   case "wire": b.Data=[]byte("[2,\"id-1\",\"StartTransaction\",{\"connectorId\":2}]")
   }
   if !errors.Is(d.SendRequest(b),errQuarantined){t.Fatal("unsafe call accepted")}
   if sink.count()!=0{t.Fatal("unsafe call delegated")}
   if !errors.Is(d.SendRequest(frame(t,req,"id-1")),errQuarantined){t.Fatal("binding reused")}
  })
 }
}
func TestLAB6DuplicateAttemptAndUID(t *testing.T){
 for _,kind:=range []string{"attempt","uid"}{
  t.Run(kind,func(t *testing.T){
   dir:=newJournal(t);sink:=&fakeQueue{dir:dir};d:=newUIDDispatcher(dir,sink)
   a:=request()
   if err:=d.bind(a,newIntent("a","pending-bind"));err!=nil{t.Fatal(err)}
   if err:=d.SendRequest(frame(t,a,"id-1"));err!=nil{t.Fatal(err)}
   b:=request();attempt,uid:="a","id-2"
   if kind=="uid"{attempt="b";uid="id-1"}
   if err:=d.bind(b,newIntent(attempt,"pending-bind"));err!=nil{t.Fatal(err)}
   if !errors.Is(d.SendRequest(frame(t,b,uid)),errQuarantined){t.Fatal("duplicate accepted")}
   if sink.count()!=1{t.Fatal("duplicate delegated")}
  })
 }
}
func TestLAB6LostACKAndQueueFailureRemainUncertain(t *testing.T){
 dir:=newJournal(t);sink:=&fakeQueue{dir:dir,fail:true};d:=newUIDDispatcher(dir,sink)
 req:=request()
 if err:=d.bind(req,newIntent("lost","pending-bind"));err!=nil{t.Fatal(err)}
 if !errors.Is(d.SendRequest(frame(t,req,"id-lost")),errQuarantined){t.Fatal("queue error not quarantined")}
 retry:=request()
 if err:=d.bind(retry,newIntent("lost","pending-bind"));err!=nil{t.Fatal(err)}
 if !errors.Is(d.SendRequest(frame(t,retry,"id-retry")),errQuarantined){t.Fatal("unsafe retry")}
 if sink.count()!=1{t.Fatal("retry delegated")}
}
func TestLAB6ConcurrentIndependentAttempts(t *testing.T){
 dir:=newJournal(t);sink:=&fakeQueue{dir:dir};d:=newUIDDispatcher(dir,sink)
 const n=20
 errs:=make(chan error,n);var wg sync.WaitGroup
 for k:=0;k<n;k++{
  wg.Add(1);go func(k int){
   defer wg.Done();req:=request()
   if err:=d.bind(req,newIntent(fmt.Sprintf("a-%d",k),"pending-bind"));err!=nil{errs<-err;return}
   call:=&ocppj.Call{MessageTypeId:ocppj.CALL,UniqueId:fmt.Sprintf("id-%d",k),Action:core.StartTransactionFeatureName,Payload:req}
   data,err:=call.MarshalJSON()
   if err==nil{err=d.SendRequest(ocppj.RequestBundle{Call:call,Data:data})}
   errs<-err
  }(k)
 }
 wg.Wait();close(errs)
 for err:=range errs{if err!=nil{t.Fatal(err)}}
 if sink.count()!=n{t.Fatalf("delegations %d",sink.count())}
}
func TestLAB6WrongScopeAndStorageFailure(t *testing.T){
 for _,mutate:=range []func(*intent){
  func(i *intent){i.SourceID="production"},
  func(i *intent){i.Tenant="real"},
  func(i *intent){i.CP="real"},
  func(i *intent){i.Epoch=0},
  func(i *intent){i.DatabaseIncarnation="existing"},
 }{
  i:=newIntent("a","id");mutate(&i)
  if !errors.Is(reserve(newJournal(t),i),errQuarantined){t.Fatal("bad scope accepted")}
 }
 dir:=newJournal(t)
 if err:=os.Chmod(dir,0755);err!=nil{t.Fatal(err)}
 if !errors.Is(reserve(dir,newIntent("a","id")),errQuarantined){t.Fatal("unsafe mode")}
 if !errors.Is(reserve(dir+"-missing",newIntent("a","id")),errQuarantined){t.Fatal("missing journal")}
}
func TestLAB6SIGKILLAfterFsync(t *testing.T){
 if os.Getenv("LAB6_CHILD_KILL")=="1"{
  if err:=reserve(os.Getenv("LAB6_JOURNAL"),newIntent("killed-attempt","killed-uid"));err!=nil{t.Fatal(err)}
  _=syscall.Kill(os.Getpid(),syscall.SIGKILL)
  os.Exit(137)
 }
 dir:=newJournal(t)
 c:=exec.Command(os.Args[0],"-test.run=TestLAB6SIGKILLAfterFsync")
 c.Env=append(os.Environ(),"LAB6_CHILD_KILL=1","LAB6_JOURNAL="+dir)
 err:=c.Run();var e *exec.ExitError
 if !errors.As(err,&e){t.Fatalf("child did not die: %v",err)}
 status,ok:=e.Sys().(syscall.WaitStatus)
 if !ok||!status.Signaled()||status.Signal()!=syscall.SIGKILL{t.Fatalf("not SIGKILL: %v",err)}
 entries,_:=os.ReadDir(dir)
 if len(entries)!=2{t.Fatalf("journal lost after SIGKILL: %d",len(entries))}
 if !errors.Is(reserve(dir,newIntent("killed-attempt","retry-id")),errQuarantined){t.Fatal("unsafe retry")}
}
func TestLAB6UnsafeCLIRejectedBeforeNetwork(t *testing.T){
 for _,args:=range [][]string{
  nil,
  {"-journal","/lab/journal","-attempt","a","-target","ws://127.0.0.1:9000"},
  {"-journal","/lab/journal","-attempt","a","-target","ws://0.0.0.0:39291"},
  {"-journal","/lab/journal","-attempt","a","-target",labEndpoint},
 }{
  if !errors.Is(run(args),errQuarantined){t.Fatalf("unsafe invocation accepted: %v",args)}
 }
}
