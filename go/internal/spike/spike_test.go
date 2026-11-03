package spike

import (
 "encoding/json"
 "fmt"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"
 "time"

 "github.com/gorilla/websocket"
)

func TestValidate(t *testing.T) {
 good:=Options{CP:"SIM001",URL:"ws://127.0.0.1:1234/SIM001",Vendor:"Arthexis",Model:"GoSpike"}
 if err:=Validate(good);err!=nil{t.Fatal(err)}
 for _,address:=range []string{"wss://127.0.0.1/SIM001","ws://example.com/SIM001","ws://127.0.0.1/OTHER","ws://user:pass@127.0.0.1/SIM001"} {
  bad:=good;bad.URL=address
  if err:=Validate(bad);err==nil{t.Errorf("accepted %s",address)}
 }
}

func TestBootAndHeartbeatIndependentWireFixture(t *testing.T) {
 actions:=make(chan string,2)
 errs:=make(chan error,1)
 upgrader:=websocket.Upgrader{Subprotocols:[]string{"ocpp1.6"}}
 srv:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  if r.URL.Path!="/SIM001" {http.Error(w,"wrong charge point",http.StatusNotFound);return}
  ws,err:=upgrader.Upgrade(w,r,nil)
  if err!=nil{errs<-err;return}
  defer ws.Close()
  if ws.Subprotocol()!="ocpp1.6"{errs<-fmt.Errorf("missing ocpp1.6 subprotocol");return}
  for i,action:=range []string{"BootNotification","Heartbeat"} {
   _=ws.SetReadDeadline(time.Now().Add(5*time.Second))
   _,message,err:=ws.ReadMessage()
   if err!=nil{errs<-err;return}
   var frame []json.RawMessage
   if err=json.Unmarshal(message,&frame);err!=nil||len(frame)!=4{errs<-fmt.Errorf("bad frame %s: %v",message,err);return}
   var kind int
   var id,actual string
   _=json.Unmarshal(frame[0],&kind)
   _=json.Unmarshal(frame[1],&id)
   _=json.Unmarshal(frame[2],&actual)
   if kind!=2||id==""||actual!=action{errs<-fmt.Errorf("expected %s, received %s",action,message);return}
   if i==0 {
    var boot map[string]any
    _=json.Unmarshal(frame[3],&boot)
    if boot["chargePointVendor"]!="Arthexis"||boot["chargePointModel"]!="GoSpike"{errs<-fmt.Errorf("unexpected boot %s",frame[3]);return}
   }
   actions<-actual
   var payload any=map[string]any{"currentTime":"2026-01-01T00:00:00Z"}
   if i==0{payload=map[string]any{"currentTime":"2026-01-01T00:00:00Z","interval":60,"status":"Accepted"}}
   if err=ws.WriteJSON([]any{3,id,payload});err!=nil{errs<-err;return}
  }
 }))
 defer srv.Close()
 options:=Options{CP:"SIM001",URL:"ws"+strings.TrimPrefix(srv.URL,"http")+"/SIM001",Vendor:"Arthexis",Model:"GoSpike"}
 done:=make(chan error,1)
 go func(){done<-Run(options)}()
 select {case err:=<-done:if err!=nil{t.Fatal(err)};case <-time.After(8*time.Second):t.Fatal("timeout")}
 select{case err:=<-errs:t.Fatal(err);default:}
 if <-actions!="BootNotification"||<-actions!="Heartbeat"{t.Fatal("wrong ordering")}
}
