// Go OCPP 1.6J client. Python remains the default public backend.
package main

import (
	"context"
	"flag"
	"fmt"
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
	if err := flags.Parse(args); err != nil { return err }
	if flags.NArg() != 0 { return fmt.Errorf("unexpected positional arguments: %v", flags.Args()) }
	if *timeout <= 0 || *timeout > 120 { return fmt.Errorf("--connect-timeout must be positive and <=120s") }
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return spike.Serve(ctx, spike.ServeOptions{
		Options: spike.Options{CP:*cp, URL:*address, Vendor:*vendor, Model:*model},
		CA:*ca, BasicUser:*basicUser, BasicPassword:*basicPassword,
		ConnectTimeout:time.Duration(*timeout*float64(time.Second)),
	})
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ocpp-simulator-go:", err)
		os.Exit(1)
	}
}
