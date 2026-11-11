package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type resetWireEvidence struct {
	resetAccepted bool
	startAccepted bool
	stopReason string
	stopTx int
	stopCount int
	availableAfter bool
	active bool
}

// This test uses an ephemeral, in-process OCPP-J fixture. No production CSMS,
// charger, Ansible run, hardware reset or external network is involved.
func TestR5OCPPJSoftHardResetAndDisconnect(t *testing.T) {
	for _,mode:=range []string{"Soft","Hard","Disconnect","Cancel"}{
		t.Run(mode,func(t *testing.T){
			done:=make(chan resetWireEvidence,1)
			failure:=make(chan error,1)
			cancelSignal:=make(chan struct{},1)
			release:=make(chan struct{})
			report:=func(err error){select{case failure<-err:default:}}
			server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
				if r.URL.Path!="/R5-LOCAL"{http.NotFound(w,r);return}
				up:=websocket.Upgrader{Subprotocols:[]string{"ocpp1.6"}}
				ws,err:=up.Upgrade(w,r,nil)
				if err!=nil{report(err);return}
				defer ws.Close()
				var evidence resetWireEvidence
				initial:=false
				startSent:=false
				resetSent:=false
				send:=func(message any)bool{
					if err:=ws.WriteJSON(message);err!=nil{report(err);return false}
					return true
				}
				for{
					_ = ws.SetReadDeadline(time.Now().Add(4*time.Second))
					_,wire,err:=ws.ReadMessage()
					if err!=nil{
						if mode=="Hard" && evidence.resetAccepted {
							done<-evidence
							<-release
						}
						if mode=="Disconnect" && evidence.active {
							done<-evidence
							<-release
						}
						return
					}
					var frame []json.RawMessage
					if err:=json.Unmarshal(wire,&frame);err!=nil||len(frame)<3{
						report(fmt.Errorf("bad OCPP-J frame: %s",wire));return
					}
					var typ int
					var id string
					if json.Unmarshal(frame[0],&typ)!=nil||json.Unmarshal(frame[1],&id)!=nil{
						report(fmt.Errorf("bad type/id: %s",wire));return
					}
					switch typ {
					case 2:
						if len(frame)!=4 {report(fmt.Errorf("invalid CALL %s",wire));return}
						var action string
						_ = json.Unmarshal(frame[2],&action)
						response:=any(map[string]any{})
						switch action {
						case "BootNotification":
							response=map[string]any{"status":"Accepted","interval":1,"currentTime":"2026-11-11T12:00:00Z"}
						case "Heartbeat":
							response=map[string]any{"currentTime":"2026-11-11T12:00:00Z"}
						case "StatusNotification":
							var p struct{Connector int `json:"connectorId"`;Status string `json:"status"`}
							if json.Unmarshal(frame[3],&p)!=nil||p.Connector!=1 {report(fmt.Errorf("bad connector status"));return}
							if !initial {
								if p.Status!="Available"{report(fmt.Errorf("not initially Available"));return}
								initial=true
							}else if p.Status=="Available" {
								evidence.availableAfter=true
							}
						case "Authorize":
							response=map[string]any{"idTagInfo":map[string]any{"status":"Accepted"}}
						case "StartTransaction":
							response=map[string]any{"transactionId":715,"idTagInfo":map[string]any{"status":"Accepted"}}
							evidence.active=true
						case "MeterValues":
							var p struct{ID int `json:"transactionId"`}
							if json.Unmarshal(frame[3],&p)!=nil||p.ID!=715{report(fmt.Errorf("wrong meter ID"));return}
						case "StopTransaction":
							var p struct{ID int `json:"transactionId"`;Reason string `json:"reason"`}
							if json.Unmarshal(frame[3],&p)!=nil||p.ID!=715{report(fmt.Errorf("wrong stop ID"));return}
							evidence.stopTx=p.ID
							evidence.stopReason=p.Reason
							evidence.stopCount++
							if mode!="Soft"&&mode!="Cancel" {report(fmt.Errorf("StopTransaction must not be sent after %s",mode));return}
						default:
							report(fmt.Errorf("unexpected charge-point action %s",action));return
						}
						if !send([]any{3,id,response}){return}
						if action=="Heartbeat"&&initial&&!startSent {
							startSent=true
							if !send([]any{2,"r5-start","RemoteStartTransaction",map[string]any{"connectorId":1,"idTag":"R5-USER"}}){return}
						}
						if action=="MeterValues"&&!resetSent {
							resetSent=true
							if mode=="Disconnect" {
								// Close while a real CSMS transaction is active; the
								// simulator must not invent a StopTransaction ACK.
								_ = ws.Close()
								done<-evidence
								<-release
								return
							}
							if mode=="Cancel" {
								select {case cancelSignal<-struct{}{}:default:}
								continue
							}
							if !send([]any{2,"r5-reset","Reset",map[string]any{"type":mode}}){return}
						}
					case 3:
						var reply struct{Status string `json:"status"`}
						if json.Unmarshal(frame[2],&reply)!=nil{report(fmt.Errorf("bad confirmation"));return}
						switch id{
						case "r5-start":
							if reply.Status!="Accepted"{report(fmt.Errorf("RemoteStart rejected"));return}
							evidence.startAccepted=true
						case "r5-reset":
							if reply.Status!="Accepted"{report(fmt.Errorf("%s Reset rejected: %s",mode,reply.Status));return}
							evidence.resetAccepted=true
						default:report(fmt.Errorf("unexpected confirmation %s",id));return
						}
					case 4:
						report(fmt.Errorf("unexpected OCPP CALLERROR %s",wire));return
					default:
						report(fmt.Errorf("unexpected type %d",typ));return
					}
					if (mode=="Soft"||mode=="Cancel")&&
						(mode=="Cancel"||evidence.resetAccepted)&&evidence.startAccepted&&
						evidence.stopCount==1&&evidence.availableAfter {
						done<-evidence
						<-release
						return
					}
				}
			}))
			defer func(){close(release);server.Close()}()
			ctx,cancel:=context.WithTimeout(context.Background(),6*time.Second)
			defer cancel()
			go func(){
				select {
				case <-cancelSignal: cancel()
				case <-ctx.Done():
				}
			}()
			serveDone:=make(chan error,1)
			go func(){
				serveDone<-Serve(ctx,ServeOptions{
					Options:Options{CP:"R5-LOCAL",URL:"ws"+strings.TrimPrefix(server.URL,"http")+"/R5-LOCAL",Vendor:"Arthexis",Model:"Simulator"},
					ConnectTimeout:time.Second,Remote:true,Connectors:1,RemotePowerKW:7.2,
					RemoteDuration:5*time.Second,RemoteMeterInterval:20*time.Millisecond,
				})
			}()
			var got resetWireEvidence
			select{
			case got=<-done:
			case err:=<-failure:t.Fatalf("wire fixture error: %v",err)
			case err:=<-serveDone:
				// Hard closes transport deliberately. The websocket reader
				// may observe closure just after Serve has returned.
				if mode!="Hard" {t.Fatalf("Serve exited before reset fixture completed: %v",err)}
				select {
				case got=<-done:
				case err:=<-failure:t.Fatalf("Hard fixture failure: %v",err)
				case <-time.After(2*time.Second):t.Fatalf("Hard fixture never saw disconnect after Serve: %v",err)
				}
				if err==nil||!strings.Contains(err.Error(),"reset") {t.Fatalf("Hard termination was silent: %v",err)}
				// Put the terminal result back for the common assertion.
				serveDone<-err
			case <-ctx.Done():t.Fatal("OCPP reset fixture timed out")
			}
			if !got.startAccepted||!got.active{t.Fatalf("did not establish real CSMS transaction: %+v",got)}
			if mode=="Soft"{
				if !got.resetAccepted||got.stopCount!=1||got.stopTx!=715||
					got.stopReason!="SoftReset"||!got.availableAfter{
					t.Fatalf("Soft did not close with confirmed SoftReset: %+v",got)
				}
			}else if mode=="Hard"{
				if !got.resetAccepted||got.stopCount!=0||got.availableAfter{
					t.Fatalf("Hard emitted a fictional stop: %+v",got)
				}
			}else if mode=="Cancel" {
				if got.stopCount!=1||got.stopReason!="Local"||!got.availableAfter {
					t.Fatalf("context cancellation failed graceful close: %+v",got)
				}
			}else if got.stopCount!=0||got.availableAfter{
				t.Fatalf("disconnect fabricated a stop: %+v",got)
			}
			select{
			case err:=<-serveDone:
				if mode=="Cancel" {
					if err!=nil {t.Fatalf("confirmed signal shutdown must be clean: %v",err)}
				}else{
					if err==nil {
						t.Fatalf("%s termination must be explicit and require reconciliation",mode)
					}
					if mode!="Disconnect"&&!strings.Contains(err.Error(),"reset"){
						t.Fatalf("missing reset termination reason: %v",err)
					}
					if mode=="Disconnect"&&!strings.Contains(err.Error(),"disconnected"){
						t.Fatalf("missing disconnect reason: %v",err)
					}
				}
			case <-time.After(4*time.Second):t.Fatal("Serve did not terminate in bounded time")
			}
		})
	}
}
