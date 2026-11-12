package spike

import (
 "testing"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
)
func TestRemoteHandlerFailsClosed(t *testing.T) {
 ledger,_:=NewConnectorLedger(2)
 h:=&RemoteHandler{Ledger:ledger}
 start,err:=h.OnRemoteStartTransaction(core.NewRemoteStartTransactionRequest("TEST001"))
 if err!=nil||start.Status!=types.RemoteStartStopStatusRejected {t.Fatalf("unsafe remote start: %+v %v",start,err)}
 stop,err:=h.OnRemoteStopTransaction(&core.RemoteStopTransactionRequest{TransactionId:42})
 if err!=nil||stop.Status!=types.RemoteStartStopStatusRejected {t.Fatalf("unsafe remote stop: %+v %v",stop,err)}
 reset,err:=h.OnReset(&core.ResetRequest{})
 if err!=nil||reset.Status!=core.ResetStatusRejected {t.Fatalf("unsafe reset: %+v %v",reset,err)}
 if _,ok:=ledger.ConnectorFor(42);ok {t.Fatal("invented transaction")}
}
func TestRemoteConfigurationValidation(t *testing.T) {
 h:=&RemoteHandler{}
 for _,value:=range []string{"0","-1","bad","86401"} {
  r,err:=h.OnChangeConfiguration(&core.ChangeConfigurationRequest{Key:"HeartbeatInterval",Value:value})
  if err!=nil||r.Status!=core.ConfigurationStatusRejected {t.Fatalf("accepted %q: %+v %v",value,r,err)}
 }
 r,err:=h.OnChangeConfiguration(&core.ChangeConfigurationRequest{Key:"HeartbeatInterval",Value:"30"})
 if err!=nil||r.Status!=core.ConfigurationStatusAccepted {t.Fatal(r,err)}
 config,err:=h.OnGetConfiguration(&core.GetConfigurationRequest{Key:[]string{"HeartbeatInterval","missing"}})
 if err!=nil||len(config.ConfigurationKey)!=1||*config.ConfigurationKey[0].Value!="30"||len(config.UnknownKey)!=1 {t.Fatalf("wrong configuration %+v %v",config,err)}
 r,err=h.OnChangeConfiguration(&core.ChangeConfigurationRequest{Key:"AuthorizeRemoteTxRequests",Value:"false"})
 if err!=nil||r.Status!=core.ConfigurationStatusNotSupported {t.Fatal(r,err)}
}

func TestHeartbeatConfigurationNotifiesServingLoop(t *testing.T) {
 updates:=make(chan int,1)
 h:=&RemoteHandler{heartbeatUpdates:updates}
 for _,n:=range []string{"15","30","45"} {
  r,err:=h.OnChangeConfiguration(&core.ChangeConfigurationRequest{Key:"HeartbeatInterval",Value:n})
  if err!=nil||r.Status!=core.ConfigurationStatusAccepted {t.Fatal(r,err)}
 }
 select {case n:=<-updates:if n!=45 {t.Fatalf("stale heartbeat update %d",n)}
 default:t.Fatal("no heartbeat update")}
}
