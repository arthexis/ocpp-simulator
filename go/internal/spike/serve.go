package spike

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net"
	"testing"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/arthexis/ocpp-simulator/go/internal/transport"
	ocpp16 "github.com/lorenzodonini/ocpp-go/ocpp1.6"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
)

type ServeOptions struct {
	Options
	CA, BasicUser, BasicPassword string
	ConnectTimeout time.Duration
	Charge *ChargeOptions
 Remote bool
 Connectors int
 RemotePowerKW float64
 RemoteDuration time.Duration
 RemoteMeterInterval time.Duration
 RemoteBatteryKWh float64
 RemoteBatterySOC float64
 RemoteBatterySeed uint64
 // RecoveryRoot is a private absolute directory, intentionally separate
 // from releases and all existing CSMS/collector SQLite databases.
 RecoveryRoot string
 RecoveryTenant string
 // Unexported offline-only proof of a synthetic loopback CSMS. The normal
 // executable cannot enable this path, even with environment variables.
 testOnlyLoopback bool
}

var cpName = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

func ValidateServe(o ServeOptions) error {
	if !cpName.MatchString(o.CP) || o.CP == "." || o.CP == ".." {
		return fmt.Errorf("invalid charge-point identifier")
	}
	if len(o.Vendor) == 0 || len(o.Vendor) > 20 || len(o.Model) == 0 || len(o.Model) > 20 {
		return fmt.Errorf("vendor/model must contain 1-20 characters")
	}
	u, err := url.Parse(o.URL)
	if err != nil || u == nil || (u.Scheme != "ws" && u.Scheme != "wss") ||
		u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		u.EscapedPath() == "" || !strings.HasSuffix(u.Path, "/"+o.CP) {
		return fmt.Errorf("--url requires a full ws:// or wss:// URL ending in /<cp>, without embedded credentials")
	}
	if o.CA != "" && u.Scheme != "wss" {
		return fmt.Errorf("--ca requires wss://")
	}
	if o.BasicUser != "" && o.BasicPassword == "" {
		return fmt.Errorf("--basic-user requires --basic-password")
	}
	if o.BasicPassword != "" && u.Scheme != "wss" {
		return fmt.Errorf("--basic-password requires wss://")
	}
	if o.ConnectTimeout <= 0 || o.ConnectTimeout > 120*time.Second {
		return fmt.Errorf("--connect-timeout must be between 0 and 120 seconds")
	}
	if o.Charge != nil { if err := o.Charge.Validate(); err != nil { return err } }
 if o.Remote {
  if _,err:=NewConnectorLedger(o.Connectors);err!=nil {return err}
  if _,err:=(RemoteChargeSettings{
   PowerKW:o.RemotePowerKW, Duration:o.RemoteDuration, MeterInterval:o.RemoteMeterInterval,
   BatteryKWh:o.RemoteBatteryKWh, BatterySOC:o.RemoteBatterySOC, BatterySeed:o.RemoteBatterySeed,
  }).normalized();err!=nil{return err}
 } else if o.Connectors!=0 {
  return fmt.Errorf("--connectors requires --remote")
 } else if o.RemoteBatteryKWh!=0 || o.RemoteBatterySOC!=0 || o.RemoteBatterySeed!=0 {
  return fmt.Errorf("remote battery settings require --remote")
 }
 if o.Charge!=nil && o.Remote {return fmt.Errorf("one-shot --connector and --remote are mutually exclusive")}
 if o.Remote {
   if o.testOnlyLoopback {
     host,_:=url.Parse(o.URL)
     h:=host.Hostname()
     ip:=net.ParseIP(h)
     if !testing.Testing() || !(h=="localhost" || (ip!=nil && ip.IsLoopback())) {
       return fmt.Errorf("test authority only applies to local synthetic CSMS under go test")
     }
   } else if o.RecoveryRoot=="" || o.RecoveryTenant=="" {
     return fmt.Errorf("remote recovery requires explicit --recovery-dir and --recovery-tenant; quarantined")
   }
 } else if o.RecoveryRoot!="" || o.RecoveryTenant!="" || o.testOnlyLoopback {
   return fmt.Errorf("recovery options require remote mode")
 }
	return nil
}

func (o ServeOptions) tlsConfig() (*tls.Config, error) {
	u, _ := url.Parse(o.URL)
	if u.Scheme != "wss" {
		return nil, nil
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("system root CAs: %w", err)
	}
	if o.CA != "" {
		pem, err := os.ReadFile(o.CA)
		if err != nil {
			return nil, fmt.Errorf("CA bundle: %w", err)
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA bundle contains no certificates")
		}
	}
	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, nil
}

// Serve stays connected, sends periodic Heartbeats at the interval issued by
// the CSMS, and exits without retry after cancellation or disconnection.
func Serve(ctx context.Context, o ServeOptions) (resultErr error) {
	if err := ValidateServe(o); err != nil {
		return err
	}
	if o.Remote && !o.testOnlyLoopback {
		identity:=RecoveryIdentity{Tenant:o.RecoveryTenant,CSMS:o.URL,CP:o.CP,Connectors:o.Connectors}
		store,err:=OpenSQLiteRecoveryStore(o.RecoveryRoot,identity)
		if err!=nil {return fmt.Errorf("remote recovery store: %w",err)}
		defer store.Close()
		// Startup does not reach even BootNotification until a trusted CSMS
		// reconciliation mechanism is separately implemented and approved.
		if err:=store.BeforeAvailable(ctx);err!=nil {
			return fmt.Errorf("remote startup gate: %w",err)
		}
	}
	tlsCfg, err := o.tlsConfig()
	if err != nil {
		return err
	}
	client := transport.NewCheckedClient(tlsCfg, o.ConnectTimeout)
	if o.BasicPassword != "" {
		user := o.BasicUser
		if user == "" { user = o.CP }
		client.SetBasicAuth(user, o.BasicPassword)
	}
	cp := ocpp16.NewChargePoint(o.CP, nil, client)
 var heartbeatUpdates chan int
 var remoteHandler *RemoteHandler
 var sessions *SessionManager
 var dispatcher *RemoteStartDispatcher
 if o.Remote {
  sessions, err = NewSessionManager(ctx, o.Connectors)
  if err != nil { return err }
  defer sessions.Close()
  dispatcher, err = NewRemoteStartDispatcher(sessions, cp, RemoteChargeSettings{
   PowerKW:o.RemotePowerKW, Duration:o.RemoteDuration, MeterInterval:o.RemoteMeterInterval,
   BatteryKWh:o.RemoteBatteryKWh, BatterySOC:o.RemoteBatterySOC, BatterySeed:o.RemoteBatterySeed,
  })
  if err != nil { return err }
  heartbeatUpdates = make(chan int, 1)
  remoteHandler = &RemoteHandler{
   Ledger: sessions.ledger,
   StartRemote: dispatcher.TryStart,
   StopRemote: dispatcher.TryStop,
   ResetRemote: dispatcher.TryReset,
   heartbeatUpdates: heartbeatUpdates,
  }
  cp.SetCoreHandler(remoteHandler)
 }
	// ocpp-go's API accepts a base URL and appends /<chargePointID>.
	if err := cp.Start(strings.TrimSuffix(o.URL, "/"+o.CP)); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func(){
		if dispatcher==nil {
			cp.Stop()
			return
		}
		// Upstream OCPP stop waits for its dispatcher; a lost reply can
		// leave this wait blocked. Never let a signal hang indefinitely.
		done:=make(chan struct{})
		go func(){cp.Stop();close(done)}()
		select {
		case <-done:
		case <-time.After(time.Second):
			fmt.Fprintln(os.Stderr,"{\"event\":\"ocpp_client_stop_timeout\",\"reconnect\":\"disabled\"}")
			if resultErr==nil {
				resultErr=fmt.Errorf("OCPP client stop timeout; manual reconciliation required")
			}
		}
	}()
 if dispatcher != nil {
  // Worker shutdown precedes cp.Stop. Soft reset and signal exits allow a
  // bounded grace for confirmed closes; Hard never fabricates a close.
  defer func() {
   dispatcher.Close()
   timedOut:=false
   // Graceful Soft gets the full drain window. For an already canceled
   // signal or lost connection, use a shorter bounded fail-closed exit.
   drainGrace:=3*time.Second
   if sessions.ResetMode()!=core.ResetTypeSoft && (ctx.Err()!=nil || resultErr!=nil) {
    drainGrace=2*time.Second
   }
   select {
   case <-dispatcher.Done():
   case <-time.After(drainGrace):
    timedOut=true
    // Transport may have a response queue stuck behind an unreachable CSMS.
    // Closing the local WebSocket is safer than falsely finishing a
    // transaction. The outstanding ID stays in the ledger/evidence.
    closed:=make(chan struct{})
    go func(){client.Stop();close(closed)}()
    select {
    case <-closed:
    case <-time.After(500*time.Millisecond):
      fmt.Fprintln(os.Stderr,"{\"event\":\"ocpp_transport_stop_timeout\",\"reconnect\":\"disabled\"}")
    }
   }
   unresolved:=sessions.Unresolved()
   if timedOut || len(unresolved)>0 || sessions.ResetMode()!="" {
    evidence:=struct{
     Event string `json:"event"`
     Mode string `json:"mode"`
     Reconnect string `json:"reconnect"`
     WorkersTimedOut bool `json:"workers_timed_out"`
     Unresolved []SessionSnapshot `json:"unresolved"`
    }{Event:"ocpp_simulator_shutdown",Mode:string(sessions.ResetMode()),
      Reconnect:"disabled_until_reconciled",WorkersTimedOut:timedOut,Unresolved:unresolved}
    encoded,_:=json.Marshal(evidence)
    fmt.Fprintf(os.Stderr,"%s\n",encoded)
    if (timedOut || len(unresolved)>0) && resultErr==nil {
      resultErr=fmt.Errorf("unresolved OCPP sessions; manual reconciliation required")
    }
   }
  }()
 }
	fmt.Println("WebSocket: connected (ocpp1.6)")
	type bootResult struct {
		interval int
		err error
	}
	bootDone := make(chan bootResult, 1)
	go func() {
		boot, err := cp.BootNotification(o.Model, o.Vendor)
		if err != nil { bootDone <- bootResult{err: fmt.Errorf("BootNotification: %w", err)}; return }
		if boot == nil || boot.Status != core.RegistrationStatusAccepted {
			bootDone <- bootResult{err: fmt.Errorf("BootNotification not accepted: %v", boot)}; return
		}
		if boot.Interval <= 0 { bootDone <- bootResult{err: fmt.Errorf("BootNotification interval must be positive")}; return }
		bootDone <- bootResult{interval: boot.Interval}
	}()
	var interval int
	select {
	case <-ctx.Done(): return nil
	case err := <-client.Disconnected(): return fmt.Errorf("disconnected before boot: %w", err)
	case result := <-bootDone:
		if result.err != nil { return result.err }
		interval = result.interval
	}
	fmt.Printf("BootNotification: Accepted (heartbeat %ds)\n", interval)
 if sessions != nil {
  if err := sessions.AnnounceInitialAvailable(ctx, func(connector int) error {
   _, statusErr := cp.StatusNotification(connector, core.NoError, core.ChargePointStatusAvailable)
   return statusErr
  }); err != nil {
   return fmt.Errorf("initial remote connector status: %w", err)
  }
  if err := dispatcher.Enable(); err != nil {
   return fmt.Errorf("enable remote starts: %w",err)
  }
 }
	if remoteHandler!=nil {remoteHandler.mu.Lock();remoteHandler.heartbeat=interval;remoteHandler.mu.Unlock()}
 ticker := time.NewTicker(time.Duration(interval)*time.Second)
	defer ticker.Stop()
	var chargingDone <-chan error
 var remoteErrors <-chan error
 var resets <-chan core.ResetType
 if dispatcher != nil {
  remoteErrors = dispatcher.Errors()
  resets = dispatcher.ResetEvents()
 }
	if o.Charge != nil {
		finished := make(chan error,1)
		chargingDone = finished
		go func(){ _, chargeErr := Charge(ctx,cp,*o.Charge); finished <- chargeErr }()
	}
	for {
		select {
		case <-ctx.Done():
			if chargingDone != nil {
				// Permit an interrupted one-shot transaction to send StopTransaction.
				select {
				case err := <-chargingDone: return err
				case <-time.After(15*time.Second): return fmt.Errorf("charge shutdown timeout; transaction may remain open")
				}
			}
			return nil
		case err := <-client.Disconnected(): return fmt.Errorf("CSMS disconnected: %w", err)
        case mode := <-resets:
         // OCPP handlers do not expose confirmation-flushed callbacks.
         // Allow the accepted ResetConfirmation a bounded chance to leave
         // the WebSocket before terminating this client. No auto reconnect.
         select {
         case <-time.After(100*time.Millisecond):
         case <-ctx.Done():
         }
         return fmt.Errorf("OCPP %s reset: controlled termination; automatic reconnect disabled", mode)
		case err := <-chargingDone: return err
        case remoteErr := <-remoteErrors:
         fmt.Fprintf(os.Stderr,"remote session: %v\n",remoteErr)
		case updated:=<-heartbeatUpdates:
            ticker.Reset(time.Duration(updated)*time.Second)
        case <-ticker.C:
			type response struct{ err error }
			reply := make(chan response, 1)
			go func() {
				hb, err := cp.Heartbeat()
				if err != nil { reply <- response{fmt.Errorf("Heartbeat: %w", err)}; return }
				if hb == nil || hb.CurrentTime == nil { reply <- response{fmt.Errorf("Heartbeat response missing currentTime")}; return }
				fmt.Printf("Heartbeat: %s\n", hb.CurrentTime.String())
				reply <- response{}
			}()
			select {
			case <-ctx.Done(): return nil
			case err := <-client.Disconnected(): return fmt.Errorf("CSMS disconnected: %w", err)
			case result := <-reply:
				if result.err != nil { return result.err }
			}
		}
	}
}
