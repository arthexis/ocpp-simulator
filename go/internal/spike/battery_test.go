package spike

import (
 "math"
 "testing"
 "time"
)

func TestBatteryCapacityAndTaper(t *testing.T) {
 b, err := NewBattery(60, 30)
 if err != nil { t.Fatal(err) }
 if b.SOC() != 30 || b.PowerKW(7.2) != 7.2 { t.Fatalf("initial battery: %+v", b) }
 b.EnergyWh = b.CapacityWh * 0.9
 if math.Abs(b.PowerKW(7.2)-3.6) > 1e-9 { t.Fatalf("90%% taper: %g", b.PowerKW(7.2)) }
 b.EnergyWh = b.CapacityWh * 0.999
 if math.Abs(b.PowerKW(7.2)-0.576) > 1e-9 { t.Fatalf("taper floor: %g", b.PowerKW(7.2)) }
 b.EnergyWh = b.CapacityWh
 if !b.Full() || b.PowerKW(7.2) != 0 { t.Fatal("full battery must not charge") }
}

func TestBatteryStepAndCapacityBound(t *testing.T) {
 b, err := NewBattery(1, 0)
 if err != nil { t.Fatal(err) }
 wh, err := b.Advance(7.2, time.Minute)
 if err != nil { t.Fatal(err) }
 if wh != 120 || b.EnergyWh != 120 || b.SOC() != 12 { t.Fatalf("energy accounting: %g %+v", wh, b) }
 b.EnergyWh = 999
 wh, err = b.Advance(7.2, time.Hour)
 if err != nil { t.Fatal(err) }
 if wh <= 0 || wh > 1 || b.EnergyWh != 1000 || !b.Full() { t.Fatalf("capacity bound: %g %+v", wh, b) }
 wh, err = b.Advance(7.2, time.Second)
 if err != nil || wh != 0 { t.Fatalf("full battery advanced: %g %v", wh, err) }
}

func TestBatteryInvalidInputs(t *testing.T) {
 for _, v := range [][2]float64{{0,30},{-1,30},{60,-1},{60,101},{math.NaN(),30},{60,math.Inf(1)}} {
  if _,err := NewBattery(v[0],v[1]); err == nil { t.Fatalf("accepted invalid battery %v",v) }
 }
 b,_ := NewBattery(60,30)
 for _, p := range []float64{0,-1,math.NaN(),math.Inf(1)} {
  if _,err := b.Advance(p,time.Second); err == nil { t.Fatalf("accepted invalid power %g",p) }
 }
 if _,err := b.Advance(7.2,-time.Second); err == nil { t.Fatal("accepted negative time") }
}

func TestBatteryDeterministicReplay(t *testing.T) {
 a,_ := NewBattery(60,79)
 b,_ := NewBattery(60,79)
 for i:=0;i<100;i++ {
  x,err := a.Advance(7.2,time.Second)
  if err != nil { t.Fatal(err) }
  y,err := b.Advance(7.2,time.Second)
  if err != nil || x != y || a.EnergyWh != b.EnergyWh { t.Fatalf("replay diverged at %d",i) }
 }
 if a.SOC() <= 79 { t.Fatal("battery did not advance") }
}

func TestSeededBatteryRepeatabilityAndBounds(t *testing.T) {
 a,_:=NewBattery(60,35)
 b,_:=NewBattery(60,35)
 other,_:=NewBattery(60,35)
 a.SetSeed(7); b.SetSeed(7); other.SetSeed(8)
 different:=false
 for i:=0;i<100;i++ {
  x,err:=a.AdvanceSeeded(7.2,time.Second)
  if err!=nil {t.Fatal(err)}
  y,err:=b.AdvanceSeeded(7.2,time.Second)
  if err!=nil || x!=y || a.EnergyWh!=b.EnergyWh {t.Fatalf("seeded replay diverged at %d",i)}
  z,err:=other.AdvanceSeeded(7.2,time.Second)
  if err!=nil {t.Fatal(err)}
  if z!=x {different=true}
  if a.drift < -0.06 || a.drift > 0.06 {t.Fatalf("drift out of bounds: %g",a.drift)}
 }
 if !different {t.Fatal("different seeds produced identical trace")}
}

func TestMeterRoundingAndInvalidDelta(t *testing.T) {
 b,_:=NewBattery(60,30)
 initial:=b.EnergyWh
 for _,tc:=range []struct{ delta float64; want int }{{0.5,0},{1.5,2},{2.5,2},{3.5,4}} {
  b.EnergyWh=initial+tc.delta
  got,err:=b.MeterWh(initial)
  if err!=nil || got!=tc.want {t.Fatalf("delta %g: %d %v",tc.delta,got,err)}
 }
 for _,v:=range []float64{initial+10,math.NaN(),math.Inf(1)} {
  if _,err:=b.MeterWh(v);err==nil {t.Fatalf("accepted invalid initial meter %g",v)}
 }
}

func TestSeededFullAndInvalidSteps(t *testing.T) {
 b,_:=NewBattery(0.001,95)
 b.SetSeed(2)
 delivered,err:=b.AdvanceSeeded(7.2,time.Hour)
 if err!=nil || delivered<=0 || !b.Full() || b.SOC()!=100 {t.Fatalf("full: %g %v %+v",delivered,err,b)}
 if more,err:=b.AdvanceSeeded(7.2,time.Second);err!=nil || more!=0 {t.Fatalf("charged full battery: %g %v",more,err)}
 if _,err:=b.AdvanceSeeded(7.2,-time.Second);err==nil {t.Fatal("accepted negative step")}
 for _,power:=range []float64{0,-1,math.NaN(),math.Inf(1)} {
  if _,err:=b.AdvanceSeeded(power,time.Second);err==nil {t.Fatalf("accepted invalid power %g",power)}
 }
}
