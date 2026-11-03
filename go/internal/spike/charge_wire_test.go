package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Wire-level CSMS fixture does not import the Go OCPP library. This validates
// encoded messages and event ordering independently of the charge-point API.
func TestG2WireTransaction(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	var energies []int
	var finalWh int
	upgrader:=websocket.Upgrader{Subprotocols:[]string{"ocpp1.6"}}
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.URL.Path!="/SIM001" {http.Error(w,"bad id",404);return}
		conn,err:=upgrader.Upgrade(w,r,nil);if err!=nil{return}
		defer conn.Close()
		for {
			_ = conn.SetReadDeadline(time.Now().Add(5*time.Second))
			_,data,err:=conn.ReadMessage();if err!=nil{return}
			var frame []json.RawMessage
			if err:=json.Unmarshal(data,&frame);err!=nil||len(frame)!=4{return}
			var id,action string
			_ = json.Unmarshal(frame[1],&id)
			_ = json.Unmarshal(frame[2],&action)
			var body map[string]any
			_ = json.Unmarshal(frame[3],&body)
			mu.Lock()
			actions=append(actions,action)
			if action=="MeterValues" {
				tx,ok:=body["transactionId"].(float64)
				if !ok||tx!=42 {t.Errorf("wrong transaction ID: %v",body)}
				values,ok:=body["meterValue"].([]any)
				if ok&&len(values)>0 {
					sampled:=values[0].(map[string]any)["sampledValue"].([]any)[0].(map[string]any)
					var wh int
					fmt.Sscan(sampled["value"].(string),&wh)
					energies=append(energies,wh)
				}
			}
			if action=="StopTransaction" {
				id,ok:=body["transactionId"].(float64)
				if !ok||id!=42 {t.Errorf("wrong stop ID: %v",body)}
				finalWh=int(body["meterStop"].(float64))
			}
			mu.Unlock()
			var response any=map[string]any{}
			switch action {
			case "BootNotification": response=map[string]any{"status":"Accepted","interval":60,"currentTime":"2026-11-03T00:00:00Z"}
			case "Authorize": response=map[string]any{"idTagInfo":map[string]any{"status":"Accepted"}}
			case "StartTransaction": response=map[string]any{"idTagInfo":map[string]any{"status":"Accepted"},"transactionId":42}
			case "Heartbeat": response=map[string]any{"currentTime":"2026-11-03T00:00:00Z"}
			case "StatusNotification","MeterValues","StopTransaction":
			default: t.Errorf("unexpected OCPP action %s",action);return
			}
			if err:=conn.WriteJSON([]any{3,id,response});err!=nil{return}
		}
	}))
	defer server.Close()
	ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel()
	o:=ServeOptions{Options:Options{CP:"SIM001",URL:"ws"+strings.TrimPrefix(server.URL,"http")+"/SIM001",Vendor:"Arthexis",Model:"Simulator"},ConnectTimeout:time.Second,
		Charge:&ChargeOptions{Connector:1,RFID:"TEST001",PowerKW:7.2,Duration:1200*time.Millisecond,MeterInterval:400*time.Millisecond}}
	if err:=Serve(ctx,o);err!=nil{t.Fatal(err)}
	mu.Lock();defer mu.Unlock()
	expected:=[]string{"BootNotification","StatusNotification","Authorize","StartTransaction","StatusNotification","MeterValues","MeterValues","MeterValues","StopTransaction","StatusNotification"}
	if len(actions)!=len(expected){t.Fatalf("actions %v expected %v",actions,expected)}
	for i,action:=range expected {if actions[i]!=action{t.Fatalf("actions %v expected %v",actions,expected)}}
	for i,v:=range energies {if i>0&&v<energies[i-1]{t.Fatalf("meter decreases: %v",energies)}}
	if finalWh!=EnergyWh(7.2,1200*time.Millisecond){t.Fatalf("final Wh %d",finalWh)}
	if energies[len(energies)-1]!=finalWh{t.Fatalf("final sample %v vs stop %d",energies,finalWh)}
}
