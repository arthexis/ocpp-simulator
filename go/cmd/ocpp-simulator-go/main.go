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
 remote := flags.Bool("remote", false, "enable bounded RemoteStartTransaction; RemoteStop/Reset remain rejected")
 connectors := flags.Int("connectors", 0, "number of connectors in remote mode (1-128)")
	rfid := flags.String("rfid", "TEST001", "authorization identifier")
	power := flags.Float64("power", 7.2, "charging power ceiling in kW")
	duration := flags.Float64("duration", 60, "one-shot charging duration in seconds")
	meterInterval := flags.Float64("meter-interval", 1, "meter sample interval in seconds")
    batteryKWh := flags.Float64("battery-kwh", 0, "enable finite battery model with this capacity in kWh")
    soc := flags.Float64("soc", 30, "initial battery state of charge in percent")
    seed := flags.Uint64("seed", 1, "deterministic battery variation seed")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) { return nil }
		return err
	}
	if flags.NArg() != 0 { return fmt.Errorf("unexpected positional arguments: %v", flags.Args()) }
	if *timeout <= 0 || *timeout > 120 { return fmt.Errorf("--connect-timeout must be positive and <=120s") }
	if *connector < 0 || *duration < 0 || *meterInterval <= 0 || *duration > 86400 || *meterInterval > 86400 {
		return fmt.Errorf("invalid charging duration or interval")
	}
	if math.IsNaN(*batteryKWh) || math.IsInf(*batteryKWh,0) || *batteryKWh < 0 { return fmt.Errorf("invalid --battery-kwh") }
    if *batteryKWh > 0 && *connector == 0 { return fmt.Errorf("--battery-kwh requires --connector") }
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return spike.Serve(ctx, spike.ServeOptions{
		Options: spike.Options{CP:*cp, URL:*address, Vendor:*vendor, Model:*model},
		CA:*ca, BasicUser:*basicUser, BasicPassword:*basicPassword,
		ConnectTimeout:time.Duration(*timeout*float64(time.Second)), Charge:charge,
  Remote:*remote, Connectors:*connectors, RemotePowerKW:*power,
  RemoteDuration:time.Duration(*duration*float64(time.Second)),
  RemoteMeterInterval:time.Duration(*meterInterval*float64(time.Second)),
	})
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ocpp-simulator-go:", err)
		os.Exit(1)
	}
}
