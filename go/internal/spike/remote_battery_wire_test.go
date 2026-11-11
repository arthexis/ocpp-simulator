package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestR4TwoIndependentBatteriesOCPPJLoopback(t *testing.T) {
	// Two real OCPP-J RemoteStart flows to an in-process synthetic CSMS only.
	// Connector 1 receives RemoteStop while its meter ACK is withheld;
	// connector 2 reaches 100% SoC and closes naturally.
	type summary struct {
		initial map[int]bool
		startAck map[string]bool
		meters map[int]int
		lastWh map[int]int
		lastSOC map[int]float64
		stopCount map[int]int
		stopped map[int]bool
		available map[int]bool
		remoteAck bool
	}
	results:=make(chan summary,1)
	failures:=make(chan error,1)
	hold:=make(chan struct{})
	defer close(hold)
	report:=func(err error){select{case failures<-err:default:}}
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.URL.Path!="/R4-TEST"{http.NotFound(w,r);return}
		upgrader:=websocket.Upgrader{Subprotocols:[]string{"ocpp1.6"}}
		ws,err:=upgrader.Upgrade(w,r,nil)
		if err!=nil{report(err);return}
		defer ws.Close()
		result:=summary{
			initial:map[int]bool{},startAck:map[string]bool{},meters:map[int]int{},
			lastWh:map[int]int{},lastSOC:map[int]float64{},stopCount:map[int]int{},
			stopped:map[int]bool{},available:map[int]bool{},
		}
		requested:=false
		remoteStopRequested:=false
		pendingMeterID:=""
		write:=func(frame any)bool{
			if err:=ws.WriteJSON(frame);err!=nil{report(err);return false}
			return true
		}
		for{
			_ = ws.SetReadDeadline(time.Now().Add(5*time.Second))
			_,wire,err:=ws.ReadMessage()
			if err!=nil{return}
			var fields []json.RawMessage
			if err:=json.Unmarshal(wire,&fields);err!=nil||len(fields)<3{
				report(fmt.Errorf("invalid OCPP frame %s",wire));return
			}
			var typ int
			var id string
			if json.Unmarshal(fields[0],&typ)!=nil||json.Unmarshal(fields[1],&id)!=nil{
				report(fmt.Errorf("invalid type or id %s",wire));return
			}
			switch typ {
			case 2:
				if len(fields)!=4 {report(fmt.Errorf("bad CALL %s",wire));return}
				var action string
				_ = json.Unmarshal(fields[2],&action)
				var response any=map[string]any{}
				var connector int
				switch action {
				case "BootNotification":
					response=map[string]any{"status":"Accepted","interval":1,"currentTime":"2026-11-11T12:00:00Z"}
				case "Heartbeat":
					response=map[string]any{"currentTime":"2026-11-11T12:00:01Z"}
				case "StatusNotification":
					var payload struct{
						Connector int `json:"connectorId"`
						Status string `json:"status"`
					}
					if json.Unmarshal(fields[3],&payload)!=nil||payload.Connector<1||payload.Connector>2 {
						report(fmt.Errorf("bad connector status %s",fields[3]));return
					}
					connector=payload.Connector
					if !result.initial[connector] {
						if payload.Status!="Available" {
							report(fmt.Errorf("missing initial Available connector %d",connector));return
						}
						result.initial[connector]=true
					}else if payload.Status=="Available"{
						result.available[connector]=true
					}else if payload.Status!="Preparing"&&payload.Status!="Charging" {
						report(fmt.Errorf("unexpected status %s",payload.Status));return
					}
				case "Authorize":
					response=map[string]any{"idTagInfo":map[string]any{"status":"Accepted"}}
				case "StartTransaction":
					var payload struct{ Connector int `json:"connectorId"` }
					if json.Unmarshal(fields[3],&payload)!=nil||payload.Connector<1||payload.Connector>2 {
						report(fmt.Errorf("bad start %s",fields[3]));return
					}
					response=map[string]any{"transactionId":1200+payload.Connector,"idTagInfo":map[string]any{"status":"Accepted"}}
				case "MeterValues":
					var payload struct{
						Connector int `json:"connectorId"`
						Transaction int `json:"transactionId"`
						Values []struct{
							Samples []struct{
								Value string `json:"value"`
								Measurand string `json:"measurand"`
								Unit string `json:"unit"`
							} `json:"sampledValue"`
						} `json:"meterValue"`
					}
					if json.Unmarshal(fields[3],&payload)!=nil || payload.Connector<1 || payload.Connector>2 ||
						payload.Transaction!=1200+payload.Connector||len(payload.Values)!=1||len(payload.Values[0].Samples)!=3{
						report(fmt.Errorf("bad battery meter %s",fields[3]));return
					}
					connector=payload.Connector
					samples:=payload.Values[0].Samples
					if samples[0].Measurand!="Energy.Active.Import.Register" || samples[0].Unit!="Wh" ||
						samples[1].Measurand!="Power.Active.Import" || samples[1].Unit!="W" ||
						samples[2].Measurand!="SoC" || samples[2].Unit!="Percent" {
						report(fmt.Errorf("invalid Wh/W/SoC measurands: %+v",samples));return
					}
					wh,e:=strconv.Atoi(samples[0].Value)
					watts,e2:=strconv.Atoi(samples[1].Value)
					soc,e3:=strconv.ParseFloat(samples[2].Value,64)
					if e!=nil||e2!=nil||e3!=nil||wh<result.lastWh[connector]||watts<0||soc<95||soc>100||
						soc<result.lastSOC[connector] {
						report(fmt.Errorf("invalid battery values: %+v",samples));return
					}
					result.lastWh[connector],result.lastSOC[connector]=wh,soc
					result.meters[connector]++
					if connector==1 && !remoteStopRequested {
						remoteStopRequested=true
						pendingMeterID=id
						// Its charging worker waits for this MeterValues ACK.
						// RemoteStop is accepted first; then energy integration
						// ends with ReasonRemote without a timing race at full.
						if !write([]any{2,"r4-stop-1","RemoteStopTransaction",map[string]any{"transactionId":1201}}){return}
						continue
					}
				case "StopTransaction":
					var payload struct{
						Transaction int `json:"transactionId"`
						Wh int `json:"meterStop"`
						Reason string `json:"reason"`
					}
					if json.Unmarshal(fields[3],&payload)!=nil||payload.Transaction<1201||payload.Transaction>1202 {
						report(fmt.Errorf("bad StopTransaction %s",fields[3]));return
					}
					connector=payload.Transaction-1200
					result.stopCount[connector]++
					wantReason:="Local"
					if connector==1 {wantReason="Remote"}
					if result.stopCount[connector]!=1||payload.Reason!=wantReason||payload.Wh!=result.lastWh[connector] {
						report(fmt.Errorf("incorrect stop %s (last Wh %d)",fields[3],result.lastWh[connector]));return
					}
					if connector==2 && result.lastSOC[2]!=100 {
						report(fmt.Errorf("connector 2 stopped before full SoC: %g",result.lastSOC[2]));return
					}
					result.stopped[connector]=true
				default:
					report(fmt.Errorf("unexpected OCPP action %s",action));return
				}
				if !write([]any{3,id,response}){return}
				if action=="Heartbeat"&&!requested&&result.initial[1]&&result.initial[2]{
					requested=true
					if !write([]any{2,"r4-start-1","RemoteStartTransaction",map[string]any{"connectorId":1,"idTag":"R4-1"}}){return}
					if !write([]any{2,"r4-start-2","RemoteStartTransaction",map[string]any{"connectorId":2,"idTag":"R4-2"}}){return}
				}
			case 3:
				var reply struct{ Status string `json:"status"` }
				if json.Unmarshal(fields[2],&reply)!=nil {report(fmt.Errorf("bad command reply %s",wire));return}
				if id=="r4-stop-1" {
					if reply.Status!="Accepted"{report(fmt.Errorf("RemoteStop rejected %s",reply.Status));return}
					result.remoteAck=true
					if pendingMeterID==""{report(fmt.Errorf("no delayed meter response"));return}
					if !write([]any{3,pendingMeterID,map[string]any{}}){return}
					pendingMeterID=""
				}else if id=="r4-start-1"||id=="r4-start-2" {
					if reply.Status!="Accepted"{report(fmt.Errorf("%s rejected %s",id,reply.Status));return}
					result.startAck[id]=true
				}else{report(fmt.Errorf("unexpected CSMS command reply %s",id));return}
			case 4:
				report(fmt.Errorf("unexpected CALLERROR %s",wire));return
			default:
				report(fmt.Errorf("unexpected message type %d",typ));return
			}
			if result.remoteAck&&result.startAck["r4-start-1"]&&result.startAck["r4-start-2"]&&
				result.stopped[1]&&result.stopped[2]&&result.available[1]&&result.available[2]&&
				result.meters[1]>0&&result.meters[2]>0 {
				results<-result
				<-hold
				return
			}
		}
	}))
	defer server.Close()
	ctx,cancel:=context.WithTimeout(context.Background(),6*time.Second)
	defer cancel()
	exited:=make(chan error,1)
	go func(){
		exited<-Serve(ctx,ServeOptions{
			Options:Options{CP:"R4-TEST",URL:"ws"+strings.TrimPrefix(server.URL,"http")+"/R4-TEST",Vendor:"Arthexis",Model:"Simulator"},
			ConnectTimeout:time.Second,Remote:true,Connectors:2,RemotePowerKW:7.2,
			RemoteDuration:2*time.Second,RemoteMeterInterval:10*time.Millisecond,
			RemoteBatteryKWh:0.00001,RemoteBatterySOC:95,RemoteBatterySeed:53,
		})
	}()
	select{
	case x:=<-results:
		if x.lastSOC[2]!=100||x.stopCount[1]!=1||x.stopCount[2]!=1 {
			t.Fatalf("incomplete per-connector battery lifecycle: %+v",x)
		}
	case err:=<-failures:t.Fatalf("local OCPP-J fixture error: %v",err)
	case err:=<-exited:t.Fatalf("Serve exited before both batteries finished: %v",err)
	case <-ctx.Done():t.Fatal("timeout in two-connector OCPP-J battery fixture")
	}
	cancel()
	select{
	case err:=<-exited:
		if err!=nil&&!strings.Contains(err.Error(),"disconnected"){t.Fatal(err)}
	case <-time.After(3*time.Second):t.Fatal("Serve did not cancel")
	}
	select{case err:=<-failures:t.Fatalf("fixture error: %v",err);default:}
}
