package spike

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
func Serve(ctx context.Context, o ServeOptions) error {
	if err := ValidateServe(o); err != nil {
		return err
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
   heartbeatUpdates: heartbeatUpdates,
  }
  cp.SetCoreHandler(remoteHandler)
 }
	// ocpp-go's API accepts a base URL and appends /<chargePointID>.
	if err := cp.Start(strings.TrimSuffix(o.URL, "/"+o.CP)); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer cp.Stop()
 if dispatcher != nil {
  // The worker shutdown runs before cp.Stop (LIFO defer order).
  // Bounded waiting lets in-flight StopTransaction finish when connected.
  defer func() {
   dispatcher.Close()
   select {
   case <-dispatcher.Done():
   case <-time.After(3*time.Second):
    fmt.Fprintln(os.Stderr,"remote worker shutdown timed out; unconfirmed transactions remain in local evidence")
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
 if dispatcher != nil {
  remoteErrors = dispatcher.Errors()
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
