package spike

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestG1RejectsUnsafeOptions(t *testing.T) {
	valid := ServeOptions{Options: Options{CP:"SIM001",URL:"wss://example.test/SIM001",Vendor:"Arthexis",Model:"Simulator"},ConnectTimeout:time.Second}
	if err:=ValidateServe(valid);err!=nil {t.Fatal(err)}
	for _,change:=range []func(*ServeOptions){
		func(o *ServeOptions){o.CP="../bad"},
		func(o *ServeOptions){o.URL="http://example.test/SIM001"},
		func(o *ServeOptions){o.URL="wss://user:pass@example.test/SIM001"},
		func(o *ServeOptions){o.URL="wss://example.test/OTHER"},
		func(o *ServeOptions){o.URL="wss://example.test/SIM001?secret=x"},
		func(o *ServeOptions){o.URL="ws://localhost/SIM001";o.BasicPassword="secret"},
		func(o *ServeOptions){o.URL="ws://localhost/SIM001";o.CA="cert.pem"},
		func(o *ServeOptions){o.BasicUser="user"},
		func(o *ServeOptions){o.ConnectTimeout=0},
	}{
		bad:=valid
		change(&bad)
		if err:=ValidateServe(bad);err==nil {t.Errorf("accepted %#v",bad)}
	}
}

func wireHandler(t *testing.T, accepted bool, count *atomic.Int32, cancel context.CancelFunc) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter,r *http.Request) {
		if r.URL.Path!="/SIM001" {http.Error(w,"not found",404);return}
		up:=websocket.Upgrader{}
		if accepted {up.Subprotocols=[]string{"ocpp1.6"}}
		ws,err:=up.Upgrade(w,r,nil)
		if err!=nil{return}
		defer ws.Close()
		for {
			_ = ws.SetReadDeadline(time.Now().Add(5*time.Second))
			_,packet,err:=ws.ReadMessage()
			if err!=nil{return}
			var frame []json.RawMessage
			if err:=json.Unmarshal(packet,&frame);err!=nil||len(frame)!=4{return}
			var action,id string
			_ = json.Unmarshal(frame[1],&id)
			_ = json.Unmarshal(frame[2],&action)
			var payload any
			switch action {
			case "BootNotification":
				payload=map[string]any{"currentTime":"2026-11-03T17:00:00Z","interval":1,"status":"Accepted"}
			case "Heartbeat":
				payload=map[string]any{"currentTime":"2026-11-03T17:00:01Z"}
				if count.Add(1)>=2 && cancel!=nil {defer cancel()}
			default: return
			}
			if err:=ws.WriteJSON([]any{3,id,payload});err!=nil{return}
		}
	}
}

func TestG1PeriodicHeartbeatAndGracefulCancel(t *testing.T) {
	var count atomic.Int32
	ctx,cancel:=context.WithTimeout(context.Background(),6*time.Second)
	defer cancel()
	server:=httptest.NewServer(wireHandler(t,true,&count,cancel))
	defer server.Close()
	err:=Serve(ctx,ServeOptions{
		Options:Options{CP:"SIM001",URL:"ws"+strings.TrimPrefix(server.URL,"http")+"/SIM001",Vendor:"Arthexis",Model:"Simulator"},
		ConnectTimeout:2*time.Second,
	})
	if err!=nil {t.Fatal(err)}
	if got:=count.Load();got<2 {t.Fatalf("got only %d heartbeats",got)}
}

func TestG1RequiresNegotiatedSubprotocol(t *testing.T) {
	server:=httptest.NewServer(wireHandler(t,false,&atomic.Int32{},nil))
	defer server.Close()
	ctx,cancel:=context.WithTimeout(context.Background(),2*time.Second)
	defer cancel()
	err:=Serve(ctx,ServeOptions{Options:Options{CP:"SIM001",URL:"ws"+strings.TrimPrefix(server.URL,"http")+"/SIM001",Vendor:"Arthexis",Model:"Simulator"},ConnectTimeout:time.Second})
	if err==nil || !strings.Contains(err.Error(),"subprotocol") {t.Fatalf("expected strict subprotocol rejection, got %v",err)}
}

func TestG1WSSCertificateAndBasicAuthentication(t *testing.T) {
	var count atomic.Int32
	ctx,cancel:=context.WithTimeout(context.Background(),6*time.Second)
	defer cancel()
	inner:=wireHandler(t,true,&count,cancel)
	server:=httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		user,pass,ok:=r.BasicAuth()
		if !ok||user!="SIM001"||pass!="test-secret" {
			http.Error(w,"unauthorized",http.StatusUnauthorized)
			return
		}
		inner(w,r)
	}))
	defer server.Close()
	options:=ServeOptions{
		Options:Options{CP:"SIM001",URL:"wss"+strings.TrimPrefix(server.URL,"https")+"/SIM001",Vendor:"Arthexis",Model:"Simulator"},
		BasicPassword:"test-secret", ConnectTimeout:2*time.Second,
	}
	// An untrusted self-signed test certificate must fail closed.
	if err:=Serve(ctx,options);err==nil {t.Fatal("accepted untrusted TLS certificate")}
	poolFile:=t.TempDir()+"/ca.pem"
	pemBytes:=pem.EncodeToMemory(&pem.Block{Type:"CERTIFICATE",Bytes:server.Certificate().Raw})
	if err:=os.WriteFile(poolFile,pemBytes,0600);err!=nil {t.Fatal(err)}
	options.CA=poolFile
	if err:=Serve(ctx,options);err!=nil {t.Fatal(err)}
	if count.Load()<2 {t.Fatal("expected multiple heartbeats with WSS and Basic Auth")}
	// Invalid credentials must fail, even with a trusted certificate.
	options.BasicPassword="wrong"
	failCtx,failCancel:=context.WithTimeout(context.Background(),2*time.Second)
	defer failCancel()
	if err:=Serve(failCtx,options);err==nil {t.Fatal("accepted invalid Basic credentials")}
}

func TestG1RejectsInvalidCA(t *testing.T) {
	p:=t.TempDir()+"/bad.pem"
	if err:=os.WriteFile(p,[]byte("not a certificate"),0600);err!=nil {t.Fatal(err)}
	o:=ServeOptions{Options:Options{CP:"SIM001",URL:"wss://localhost/SIM001",Vendor:"Arthexis",Model:"Simulator"},CA:p,ConnectTimeout:time.Second}
	if err:=Serve(context.Background(),o);err==nil {t.Fatal("accepted invalid CA bundle")}
}

var _ = x509.Certificate{}
var _ = fmt.Sprintf
