// Go OCPP 1.6J client. Python remains the default public backend.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
    "math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arthexis/ocpp-simulator/go/internal/spike"
)

func run(args []string) error {
	if len(args) > 0 && args[0] == "run" {
		args = args[1:]
	}
	flags := flag.NewFlagSet("ocpp-simulator-go run", flag.ContinueOnError)
	cp := flags.String("cp", "", "charge-point identifier")
	address := flags.String("url", "", "full ws:// or wss:// URL ending in /CP")
	vendor := flags.String("vendor", "Arthexis", "OCPP vendor")
	model := flags.String("model", "Simulator", "OCPP model")
	ca := flags.String("ca", "", "trusted CA bundle for WSS")
	basicUser := flags.String("basic-user", "", "HTTP Basic username (defaults to --cp)")
	basicPassword := flags.String("basic-password", "", "HTTP Basic password (WSS only; visible in process arguments)")
	timeout := flags.Float64("connect-timeout", 10, "WebSocket handshake timeout in seconds")
	connector := flags.Int("connector", 0, "run a single charging transaction then exit")
 remote := flags.Bool("remote", false, "enable experimental RemoteStart, RemoteStop and Soft/Hard Reset; no auto-reconnect")
 connectors := flags.Int("connectors", 0, "number of connectors in remote mode (1-128)")
 recoveryDir := flags.String("recovery-dir", "", "private absolute Go recovery state directory (remote; starts quarantined)")
 recoveryTenant := flags.String("recovery-tenant", "", "explicit tenant/source identity used for Go recovery (remote)")
	rfid := flags.String("rfid", "TEST001", "authorization identifier")
	power := flags.Float64("power", 7.2, "charging power ceiling in kW")
	duration := flags.Float64("duration", 60, "one-shot charging duration in seconds")
	meterInterval := flags.Float64("meter-interval", 1, "meter sample interval in seconds")
    batteryKWh := flags.Float64("battery-kwh", 0, "enable a finite battery per remote connector or for one-shot (kWh)")
    soc := flags.Float64("soc", 30, "initial battery state of charge in percent")
    seed := flags.Uint64("seed", 1, "deterministic battery variation seed")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) { return nil }
		return err
	}
	if flags.NArg() != 0 { return fmt.Errorf("unexpected positional arguments: %v", flags.Args()) }
	if *timeout <= 0 || *timeout > 120 { return fmt.Errorf("--connect-timeout must be positive and <=120s") }
	if *connector < 0 || math.IsNaN(*duration) || math.IsInf(*duration,0) ||
  math.IsNaN(*meterInterval) || math.IsInf(*meterInterval,0) ||
  math.IsNaN(*power) || math.IsInf(*power,0) || *power <= 0 ||
  *duration < 0 || *meterInterval <= 0 || *duration > 86400 || *meterInterval > 86400 {
		return fmt.Errorf("invalid charging duration or interval")
	}
	if math.IsNaN(*batteryKWh) || math.IsInf(*batteryKWh,0) || *batteryKWh < 0 { return fmt.Errorf("invalid --battery-kwh") }
	seen := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	if seen["battery-kwh"] && *batteryKWh <= 0 {
		return fmt.Errorf("--battery-kwh must be positive when explicitly configured")
	}
	if *batteryKWh == 0 && (seen["soc"] || seen["seed"]) {
		return fmt.Errorf("--soc and --seed require a positive --battery-kwh")
	}
	if *batteryKWh > 0 && !*remote && *connector == 0 {
		return fmt.Errorf("--battery-kwh requires --connector or --remote")
	}
	if *remote && seen["rfid"] {
		return fmt.Errorf("--rfid is only used for one-shot charging; remote RFID comes from RemoteStartTransaction")
	}
	if *batteryKWh > 0 {
		// Fail before connecting: both remote and one-shot need a valid EV.
		if _, err := spike.NewBattery(*batteryKWh,*soc); err != nil { return err }
	}
    var charge *spike.ChargeOptions
	if *connector > 0 {
		charge = &spike.ChargeOptions{Connector:*connector, RFID:*rfid, PowerKW:*power,
			Duration:time.Duration(*duration*float64(time.Second)), MeterInterval:time.Duration(*meterInterval*float64(time.Second))}
        if *batteryKWh > 0 {
            battery,err:=spike.NewBattery(*batteryKWh,*soc)
            if err!=nil {return err}
            battery.SetSeed(*seed)
            charge.Battery=&battery
            charge.Seeded=true
        }
	}
	var remoteBatteryKWh, remoteBatterySOC float64
	var remoteBatterySeed uint64
	if *remote && *batteryKWh > 0 {
		remoteBatteryKWh, remoteBatterySOC, remoteBatterySeed = *batteryKWh, *soc, *seed
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return spike.Serve(ctx, spike.ServeOptions{
		Options: spike.Options{CP:*cp, URL:*address, Vendor:*vendor, Model:*model},
		CA:*ca, BasicUser:*basicUser, BasicPassword:*basicPassword,
		ConnectTimeout:time.Duration(*timeout*float64(time.Second)), Charge:charge,
  Remote:*remote, Connectors:*connectors, RemotePowerKW:*power,
  RemoteDuration:time.Duration(*duration*float64(time.Second)),
  RemoteMeterInterval:time.Duration(*meterInterval*float64(time.Second)),
  RemoteBatteryKWh:remoteBatteryKWh, RemoteBatterySOC:remoteBatterySOC, RemoteBatterySeed:remoteBatterySeed,
  RecoveryRoot:*recoveryDir, RecoveryTenant:*recoveryTenant,
	})
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ocpp-simulator-go:", err)
		os.Exit(1)
	}
}
