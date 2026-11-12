// R6C-3B2-LAB-5: offline-only real ocpp-go v0.19.0 API test.
// This _test.go file cannot be linked into the production simulator binary.
package spike

import (
 "bytes"
 "crypto/sha256"
 "encoding/hex"
 "encoding/json"
 "errors"
 "fmt"
 "os"
 "os/exec"
 "path/filepath"
 "strings"
 "sync"
 "sync/atomic"
 "syscall"
 "testing"

 "github.com/lorenzodonini/ocpp-go/ocpp"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
 "github.com/lorenzodonini/ocpp-go/ocppj"
 "github.com/lorenzodonini/ocpp-go/ws"
)

var lab5Quarantine=errors.New("LAB-5: uncertain, no authorization")
type lab5Intent struct {
 Source string
 Database string
 Tenant string
 CP string
 Epoch int64
 Attempt string
 UID string
 Action string
}
func lab5New(attempt,uid string)lab5Intent{
 return lab5Intent{"r6c3b2-lab","r6c3b2-db","r6c3b2-tenant","lab-cp-uid",1,attempt,uid,"StartTransaction"}
}
func lab5OK(x lab5Intent)bool{
 return x.Source=="r6c3b2-lab"&&x.Database=="r6c3b2-db"&&x.Tenant=="r6c3b2-tenant"&&x.CP=="lab-cp-uid"&&x.Epoch==1&&x.Action=="StartTransaction"&&
 len(x.Attempt)>0&&len(x.Attempt)<=80&&len(x.UID)>0&&len(x.UID)<=36&&strings.TrimSpace(x.Attempt)==x.Attempt&&strings.TrimSpace(x.UID)==x.UID&&!strings.ContainsAny(x.Attempt+x.UID,"/\\\r\n\x00")
}
func lab5Name(prefix,token string)string{
 h:=sha256.Sum256([]byte(token))
 return prefix+"-"+hex.EncodeToString(h[:])+".json"
}
// O_EXCL files and fsync of each file and directory precede every enqueue.
// Partial writes remain uncertain. This is not a recovery certificate.
func lab5Reserve(dir string,x lab5Intent)error{
 if !lab5OK(x){return lab5Quarantine}
 st,err:=os.Lstat(dir)
 if err!=nil||!st.IsDir()||st.Mode()&os.ModeSymlink!=0||st.Mode().Perm()!=0700{return lab5Quarantine}
 raw,err:=json.Marshal(x);if err!=nil{return lab5Quarantine}
 raw=append(raw,'\n')
 for _,name:=range []string{lab5Name("attempt",x.Attempt),lab5Name("uid",x.UID)}{
  f,e:=os.OpenFile(filepath.Join(dir,name),os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600)
  if e!=nil{return lab5Quarantine}
  _,w:=f.Write(raw);if w==nil{w=f.Sync()}
  c:=f.Close();if w!=nil||c!=nil{return lab5Quarantine}
 }
 d,e:=os.Open(dir);if e!=nil{return lab5Quarantine}
 s:=d.Sync();c:=d.Close()
 if s!=nil||c!=nil{return lab5Quarantine}
 return nil
}
type lab5Bind struct{scope lab5Intent;payload []byte}
type lab5Dispatcher struct{
 ocppj.ClientDispatcher
 dir string
 mu sync.Mutex
 pending map[*core.StartTransactionRequest]lab5Bind
}
func lab5Wrap(dir string,next ocppj.ClientDispatcher)*lab5Dispatcher{
 return &lab5Dispatcher{ClientDispatcher:next,dir:dir,pending:make(map[*core.StartTransactionRequest]lab5Bind)}
}
func(d *lab5Dispatcher)Bind(req *core.StartTransactionRequest,x lab5Intent)error{
 if req==nil||req.ConnectorId!=1||req.IdTag!="lab-rfid"||req.MeterStart!=0||req.Timestamp==nil{return lab5Quarantine}
 // ocpp-go generates UniqueId after this binding and before SendRequest.
 x.UID="not-generated";if !lab5OK(x){return lab5Quarantine}
 raw,err:=json.Marshal(req);if err!=nil{return lab5Quarantine}
 d.mu.Lock();defer d.mu.Unlock()
 if _,ok:=d.pending[req];ok{return lab5Quarantine}
 d.pending[req]=lab5Bind{x,raw}
 return nil
}
func(d *lab5Dispatcher)SendRequest(b ocppj.RequestBundle)error{
 if b.Call==nil||b.Call.Action!=core.StartTransactionFeatureName{return lab5Quarantine}
 req,ok:=b.Call.Payload.(*core.StartTransactionRequest)
 if !ok||req==nil{return lab5Quarantine}
 d.mu.Lock();old,found:=d.pending[req];delete(d.pending,req);d.mu.Unlock()
 if !found{return lab5Quarantine}
 raw,err:=json.Marshal(req)
 if err!=nil||!bytes.Equal(raw,old.payload){return lab5Quarantine}
 var arr []json.RawMessage
 if json.Unmarshal(b.Data,&arr)!=nil||len(arr)!=4{return lab5Quarantine}
 var kind int;var uid,action string
 if json.Unmarshal(arr[0],&kind)!=nil||json.Unmarshal(arr[1],&uid)!=nil||json.Unmarshal(arr[2],&action)!=nil||kind!=2||uid!=b.Call.UniqueId||action!=b.Call.Action{return lab5Quarantine}
 var a,z any
 if json.Unmarshal(arr[3],&a)!=nil||json.Unmarshal(raw,&z)!=nil{return lab5Quarantine}
 wire,_:=json.Marshal(a);want,_:=json.Marshal(z)
 if !bytes.Equal(wire,want){return lab5Quarantine}
 old.scope.UID=uid
 if err:=lab5Reserve(d.dir,old.scope);err!=nil{return err}
 // This is the first possible call to the delegate (fake queue in tests).
 if err:=d.ClientDispatcher.SendRequest(b);err!=nil{return fmt.Errorf("%w: post-fsync queue uncertain",lab5Quarantine)}
 return nil
}
// Fake queue is deliberately disconnected. It verifies the generated UID
// was persisted before ocppj.Client.SendRequest delegates its RequestBundle.
type lab5Sink struct{
 ocppj.ClientDispatcher
 dir string
 calls atomic.Int64
 last atomic.Value
 fail bool
}
func(*lab5Sink)IsRunning()bool{return true}
func(*lab5Sink)SetNetworkClient(ws.WsClient){}
func(*lab5Sink)SetPendingRequestState(ocppj.ClientState){}
func(s *lab5Sink)SendRequest(b ocppj.RequestBundle)error{
 if b.Call==nil{return lab5Quarantine}
 raw,e:=os.ReadFile(filepath.Join(s.dir,lab5Name("uid",b.Call.UniqueId)))
 if e!=nil{return lab5Quarantine}
 var x lab5Intent
 if json.Unmarshal(raw,&x)!=nil||!lab5OK(x)||x.UID!=b.Call.UniqueId{return lab5Quarantine}
 if _,e:=os.Stat(filepath.Join(s.dir,lab5Name("attempt",x.Attempt)));e!=nil{return lab5Quarantine}
 s.calls.Add(1);s.last.Store(b.Call.UniqueId)
 if s.fail{return errors.New("simulated lost ACK")}
 return nil
}
func lab5Fixture(t *testing.T)(string,*lab5Dispatcher,*lab5Sink){
 t.Helper();dir:=filepath.Join(t.TempDir(),"journal")
 if e:=os.Mkdir(dir,0700);e!=nil{t.Fatal(e)}
 s:=&lab5Sink{dir:dir}
 return dir,lab5Wrap(dir,s),s
}
func lab5Req()*core.StartTransactionRequest{return core.NewStartTransactionRequest(1,"lab-rfid",0,types.Now())}
func lab5Bundle(t *testing.T,req *core.StartTransactionRequest,uid string)ocppj.RequestBundle{
 t.Helper()
 c:=&ocppj.Call{MessageTypeId:ocppj.CALL,UniqueId:uid,Action:core.StartTransactionFeatureName,Payload:req}
 b,e:=c.MarshalJSON();if e!=nil{t.Fatal(e)}
 return ocppj.RequestBundle{Call:c,Data:b}
}
func TestR6C3B2LAB5RealClientUID(t *testing.T){
 _,d,s:=lab5Fixture(t)
 client:=ocppj.NewClient("lab-cp-uid",ws.NewClient(),d,nil,core.Profile)
 client.SetDialect(ocpp.V16)
 req:=lab5Req()
 if e:=d.Bind(req,lab5New("real-attempt","pending"));e!=nil{t.Fatal(e)}
 if e:=client.SendRequest(req);e!=nil{t.Fatal(e)}
 if s.calls.Load()!=1||s.last.Load()==nil{t.Fatal("UID not persisted before real-client delegation")}
}
func TestR6C3B2LAB5MalformedAndUnbound(t *testing.T){
 _,d,s:=lab5Fixture(t)
 client:=ocppj.NewClient("lab-cp-uid",ws.NewClient(),d,nil,core.Profile)
 client.SetDialect(ocpp.V16)
 if !errors.Is(client.SendRequest(lab5Req()),lab5Quarantine){t.Fatal("unbound allowed")}
 req:=lab5Req()
 if e:=d.Bind(req,lab5New("bad-frame","pending"));e!=nil{t.Fatal(e)}
 b:=lab5Bundle(t,req,"uid-1");b.Data=[]byte("bad")
 if !errors.Is(d.SendRequest(b),lab5Quarantine)||!errors.Is(d.SendRequest(lab5Bundle(t,req,"uid-1")),lab5Quarantine){t.Fatal("failed binding reused")}
 if s.calls.Load()!=0{t.Fatal("invalid request delegated")}
}
func TestR6C3B2LAB5PayloadMutation(t *testing.T){
 _,d,s:=lab5Fixture(t)
 r:=lab5Req()
 if e:=d.Bind(r,lab5New("a","pending"));e!=nil{t.Fatal(e)}
 r.MeterStart=30
 if !errors.Is(d.SendRequest(lab5Bundle(t,r,"uid-1")),lab5Quarantine)||s.calls.Load()!=0{t.Fatal("mutated payload allowed")}
}
func TestR6C3B2LAB5DuplicateAndACK(t *testing.T){
 _,d,s:=lab5Fixture(t)
 a:=lab5Req()
 if e:=d.Bind(a,lab5New("a","pending"));e!=nil{t.Fatal(e)}
 if e:=d.SendRequest(lab5Bundle(t,a,"uid-a"));e!=nil{t.Fatal(e)}
 for _,pair:=range [][2]string{{"a","uid-b"},{"b","uid-a"}}{
  r:=lab5Req()
  if e:=d.Bind(r,lab5New(pair[0],"pending"));e!=nil{t.Fatal(e)}
  if !errors.Is(d.SendRequest(lab5Bundle(t,r,pair[1])),lab5Quarantine){t.Fatal("uncertain duplicate/ACK accepted")}
 }
 if s.calls.Load()!=1{t.Fatal("retry reached queue")}
}
func TestR6C3B2LAB5WrongScope(t *testing.T){
 for _,key:=range []string{"source","database","tenant","cp","epoch","action"}{
  t.Run(key,func(t *testing.T){
   dir,_,_:=lab5Fixture(t);x:=lab5New("a","uid-a")
   switch key {
    case "source":x.Source="prod"
    case "database":x.Database="prod"
    case "tenant":x.Tenant="prod"
    case "cp":x.CP="prod"
    case "epoch":x.Epoch=99
    case "action":x.Action="RemoteStartTransaction"
   }
   if !errors.Is(lab5Reserve(dir,x),lab5Quarantine){t.Fatal("wrong scope accepted")}
  })
 }
}
func TestR6C3B2LAB5QueueFailure(t *testing.T){
 _,d,s:=lab5Fixture(t);s.fail=true
 r:=lab5Req()
 if e:=d.Bind(r,lab5New("a","pending"));e!=nil{t.Fatal(e)}
 if !errors.Is(d.SendRequest(lab5Bundle(t,r,"uid-a")),lab5Quarantine){t.Fatal("post-fsync failure accepted")}
 r=lab5Req()
 if e:=d.Bind(r,lab5New("a","pending"));e!=nil{t.Fatal(e)}
 if !errors.Is(d.SendRequest(lab5Bundle(t,r,"uid-b")),lab5Quarantine)||s.calls.Load()!=1{t.Fatal("uncertain retry allowed")}
}
func TestR6C3B2LAB5Concurrent(t *testing.T){
 _,d,s:=lab5Fixture(t);ch:=make(chan error,16);var wg sync.WaitGroup
 for i:=0;i<16;i++{
  wg.Add(1)
  go func(i int){
   defer wg.Done();r:=lab5Req();id:=fmt.Sprintf("%d",i)
   e:=d.Bind(r,lab5New("a"+id,"pending"))
   if e==nil{
    c:=&ocppj.Call{MessageTypeId:ocppj.CALL,UniqueId:"uid-"+id,Action:core.StartTransactionFeatureName,Payload:r}
    raw,x:=c.MarshalJSON()
    if x!=nil{e=x}else{e=d.SendRequest(ocppj.RequestBundle{Call:c,Data:raw})}
   }
   ch<-e
  }(i)
 }
 wg.Wait();close(ch)
 for e:=range ch{if e!=nil{t.Fatal(e)}}
 if s.calls.Load()!=16{t.Fatal("lost parallel binding")}
}
func TestR6C3B2LAB5ParallelDuplicateUID(t *testing.T){
 dir,_,_:=lab5Fixture(t);var wg sync.WaitGroup;var successes atomic.Int64
 for i:=0;i<16;i++{
  wg.Add(1)
  go func(i int){
   defer wg.Done()
   if lab5Reserve(dir,lab5New(fmt.Sprintf("a-%d",i),"same"))==nil{successes.Add(1)}
  }(i)
 }
 wg.Wait()
 if successes.Load()!=1{t.Fatal("UID collision not fenced")}
}
func TestR6C3B2LAB5SIGKILL(t *testing.T){
 if os.Getenv("LAB5_CHILD")=="1"{
  if e:=lab5Reserve(os.Getenv("LAB5_DIR"),lab5New("killed","uid-killed"));e!=nil{t.Fatal(e)}
  _ = syscall.Kill(os.Getpid(),syscall.SIGKILL);os.Exit(137)
 }
 dir,_,_:=lab5Fixture(t)
 c:=exec.Command(os.Args[0],"-test.run=^TestR6C3B2LAB5SIGKILL$")
 c.Env=append(os.Environ(),"LAB5_CHILD=1","LAB5_DIR="+dir)
 e:=c.Run();var exit *exec.ExitError
 if !errors.As(e,&exit){t.Fatalf("expected killed child: %v",e)}
 st,ok:=exit.Sys().(syscall.WaitStatus)
 if !ok||!st.Signaled()||st.Signal()!=syscall.SIGKILL{t.Fatal("wrong signal")}
 files,_:=os.ReadDir(dir)
 if len(files)!=2||!errors.Is(lab5Reserve(dir,lab5New("killed","uid-second")),lab5Quarantine){t.Fatal("crash lost reservation")}
}
