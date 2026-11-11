package main

import (
	"strings"
	"testing"
)

// All scenarios fail before creating a network client. A deliberately
// unsupported URL checks that a valid flag combination passes CLI parsing.
func TestR4CLIRejectsIgnoredOrInvalidBatteryFlags(t *testing.T) {
	for _,tc:=range []struct{name string;args []string;want string}{
		{"soc without battery",[]string{"--soc","55"},"--soc and --seed require"},
		{"seed without battery",[]string{"--seed","9"},"--soc and --seed require"},
		{"zero explicit capacity",[]string{"--battery-kwh","0"},"--battery-kwh must be positive"},
		{"nan capacity",[]string{"--battery-kwh","NaN","--connector","1"},"invalid --battery-kwh"},
		{"bad SoC",[]string{"--battery-kwh","1","--soc","101","--connector","1"},"invalid battery"},
		{"battery without mode",[]string{"--battery-kwh","1"},"--battery-kwh requires --connector or --remote"},
		{"remote ignored RFID",[]string{"--remote","--connectors","2","--rfid","X"},"--rfid is only used"},
		{"invalid power",[]string{"--power","NaN"},"invalid charging duration or interval"},
		{"invalid meter",[]string{"--meter-interval","NaN"},"invalid charging duration or interval"},
		{"invalid duration",[]string{"--duration","Inf"},"invalid charging duration or interval"},
	}{
		t.Run(tc.name,func(t *testing.T){
			err:=run(tc.args)
			if err==nil||!strings.Contains(err.Error(),tc.want){
				t.Fatalf("got %v; want %q",err,tc.want)
			}
		})
	}
}

func TestR4CLIAcceptsRemoteAndOneShotBatteryFlags(t *testing.T) {
	// An invalid scheme is intentional, preventing any connection after
	// successful parsing and validation of the battery flags.
	for _,args:=range [][]string{
		{"--cp","SIM-R4","--url","http://invalid.test/SIM-R4","--remote","--connectors","2",
			"--battery-kwh","0.01","--soc","0","--seed","0"},
		{"--cp","SIM-R4","--url","http://invalid.test/SIM-R4","--connector","1",
			"--battery-kwh","0.01","--soc","95","--seed","4"},
	}{
		err:=run(args)
		if err==nil||!strings.Contains(err.Error(),"--url requires"){
			t.Fatalf("valid battery options did not reach URL validation: %v",err)
		}
	}
}
