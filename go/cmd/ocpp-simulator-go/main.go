// This command is a non-installed experiment. The Python CLI remains authoritative.
package main

import (
 "flag"
 "fmt"
 "os"

 "github.com/arthexis/ocpp-simulator/go/internal/spike"
)

func main() {
 cp:=flag.String("cp","","test charge-point ID")
 address:=flag.String("url","","loopback ws:// URL ending in /CP")
 vendor:=flag.String("vendor","Arthexis","OCPP chargePointVendor")
 model:=flag.String("model","GoSpike","OCPP chargePointModel")
 flag.Parse()
 if flag.NArg()!=0 {fmt.Fprintln(os.Stderr,"unexpected positional arguments");os.Exit(2)}
 if err:=spike.Run(spike.Options{CP:*cp,URL:*address,Vendor:*vendor,Model:*model});err!=nil{
  fmt.Fprintln(os.Stderr,err)
  os.Exit(1)
 }
}
