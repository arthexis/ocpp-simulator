package spike

import (
 "strconv"
 "sync"

 "github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
)

// RemoteHandler is a conservative OCPP 1.6 Core handler. Remote starts/stops
// remain rejected until transaction lifecycle dispatch is connected.
type RemoteHandler struct {
 Ledger *ConnectorLedger
 mu sync.Mutex
 heartbeat int
 heartbeatUpdates chan int
}
var _ core.ChargePointHandler = (*RemoteHandler)(nil)
func (h *RemoteHandler) OnRemoteStartTransaction(_ *core.RemoteStartTransactionRequest)(*core.RemoteStartTransactionConfirmation,error){
 return core.NewRemoteStartTransactionConfirmation(types.RemoteStartStopStatusRejected),nil
}
func (h *RemoteHandler) OnRemoteStopTransaction(_ *core.RemoteStopTransactionRequest)(*core.RemoteStopTransactionConfirmation,error){
 return core.NewRemoteStopTransactionConfirmation(types.RemoteStartStopStatusRejected),nil
}
func (h *RemoteHandler) OnReset(_ *core.ResetRequest)(*core.ResetConfirmation,error){
 return core.NewResetConfirmation(core.ResetStatusRejected),nil
}
func (h *RemoteHandler) OnChangeAvailability(_ *core.ChangeAvailabilityRequest)(*core.ChangeAvailabilityConfirmation,error){
 return core.NewChangeAvailabilityConfirmation(core.AvailabilityStatusRejected),nil
}
func (h *RemoteHandler) OnClearCache(_ *core.ClearCacheRequest)(*core.ClearCacheConfirmation,error){
 return core.NewClearCacheConfirmation(core.ClearCacheStatusRejected),nil
}
func (h *RemoteHandler) OnUnlockConnector(_ *core.UnlockConnectorRequest)(*core.UnlockConnectorConfirmation,error){
 return core.NewUnlockConnectorConfirmation(core.UnlockStatusNotSupported),nil
}
func (h *RemoteHandler) OnDataTransfer(_ *core.DataTransferRequest)(*core.DataTransferConfirmation,error){
 return core.NewDataTransferConfirmation(core.DataTransferStatusRejected),nil
}
func (h *RemoteHandler) OnGetConfiguration(r *core.GetConfigurationRequest)(*core.GetConfigurationConfirmation,error){
 h.mu.Lock();defer h.mu.Unlock()
 values:=map[string]string{"HeartbeatInterval":strconv.Itoa(h.heartbeat),"AuthorizeRemoteTxRequests":"true"}
 if h.heartbeat<=0 {values["HeartbeatInterval"]="60"}
 var keys []core.ConfigurationKey
 var unknown []string
 requested:=r.Key
 if len(requested)==0 {requested=[]string{"HeartbeatInterval","AuthorizeRemoteTxRequests"}}
 for _,key:=range requested {
  if value,ok:=values[key];ok {
   v:=value
   keys=append(keys,core.ConfigurationKey{Key:key,Readonly:key=="AuthorizeRemoteTxRequests",Value:&v})
  } else {unknown=append(unknown,key)}
 }
 response:=core.NewGetConfigurationConfirmation(keys)
 response.UnknownKey=unknown
 return response,nil
}
func (h *RemoteHandler) OnChangeConfiguration(r *core.ChangeConfigurationRequest)(*core.ChangeConfigurationConfirmation,error){
 if r.Key!="HeartbeatInterval" {return core.NewChangeConfigurationConfirmation(core.ConfigurationStatusNotSupported),nil}
 n,err:=strconv.Atoi(r.Value)
 if err!=nil || n<1 || n>86400 {return core.NewChangeConfigurationConfirmation(core.ConfigurationStatusRejected),nil}
 h.mu.Lock();h.heartbeat=n;updates:=h.heartbeatUpdates;h.mu.Unlock()
 if updates!=nil {select {case updates<-n:default:select {case <-updates:default:};select {case updates<-n:default:}}}
 return core.NewChangeConfigurationConfirmation(core.ConfigurationStatusAccepted),nil
}
