// Package spike provides an isolated Go/OCPP 1.6J interoperability experiment.
// It is not the installed ocpp-simulator executable.
package spike

import (
 "fmt"
 "net/url"
 "strings"
 "time"

 ocpp16 "github.com/lorenzodonini/ocpp-go/ocpp1.6"
 "github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
)

type Options struct {
 CP, URL, Vendor, Model string
}

// Validate deliberately restricts G0 to loopback plaintext fixtures.
// Public networking, WSS, credentials, and retries are deferred to G1.
func Validate(o Options) error {
 if o.CP == "" || strings.ContainsAny(o.CP, "/?#") { return fmt.Errorf("invalid --cp identifier") }
 if len(o.Vendor)==0 || len(o.Vendor)>20 || len(o.Model)==0 || len(o.Model)>20 { return fmt.Errorf("vendor/model required (max 20 chars each)") }
 u, err := url.Parse(o.URL)
 if err != nil || u == nil || u.Scheme != "ws" || u.User != nil || u.RawQuery!="" || u.Fragment!="" { return fmt.Errorf("G0 only accepts ws:// loopback URLs without credentials/query/fragment") }
 if u.Hostname()!="localhost" && u.Hostname()!="127.0.0.1" && u.Hostname()!="::1" { return fmt.Errorf("G0 connections are restricted to loopback") }
 if !strings.HasSuffix(u.Path, "/"+o.CP) { return fmt.Errorf("URL path must end with the --cp identifier") }
 return nil
}

// Run exchanges one BootNotification and one Heartbeat. No charging or services.
func Run(o Options) error {
 if err:=Validate(o);err!=nil{return err}
 cp:=ocpp16.NewChargePoint(o.CP,nil,nil)
 if err:=cp.Start(o.URL);err!=nil{return fmt.Errorf("connect: %w",err)}
 defer cp.Stop()
 boot,err:=cp.BootNotification(o.Model,o.Vendor)
 if err!=nil{return fmt.Errorf("BootNotification: %w",err)}
 if boot==nil || boot.Status!=core.RegistrationStatusAccepted {return fmt.Errorf("BootNotification not accepted: %v",boot)}
 if boot.Interval<=0 {return fmt.Errorf("BootNotification invalid heartbeat interval: %d",boot.Interval)}
 fmt.Printf("BootNotification: %s (interval %ds)\n",boot.Status,boot.Interval)
 hb,err:=cp.Heartbeat()
 if err!=nil{return fmt.Errorf("Heartbeat: %w",err)}
 if hb==nil || hb.CurrentTime==nil {return fmt.Errorf("Heartbeat response missing currentTime")}
 fmt.Printf("Heartbeat: %s\n",hb.CurrentTime.String())
 return nil
}

var _ = time.Second // intentional future heartbeat scheduling belongs to G1
