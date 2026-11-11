package spike

import (
 "context"
 "crypto/rand"
 "database/sql"
 "encoding/hex"
 "fmt"
 "math"
 "strconv"

 "github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
)

// R6C-2 records intent before an OCPP side effect. A local commit is never
// mistaken for an authoritative CSMS result. No method grants CleanVerified.
type RecoveryBattery struct {
 CapacityWh float64
 EnergyWh float64
 Seed uint64
 Drift float64
}
func (b RecoveryBattery) valid() bool {
 return !math.IsNaN(b.CapacityWh)&&!math.IsInf(b.CapacityWh,0)&&
  !math.IsNaN(b.EnergyWh)&&!math.IsInf(b.EnergyWh,0)&&
  !math.IsNaN(b.Drift)&&!math.IsInf(b.Drift,0)&&
  b.CapacityWh>=0&&b.EnergyWh>=0&&b.EnergyWh<=b.CapacityWh&&math.Abs(b.Drift)<=0.061
}
func snapshotBattery(b *remoteBatterySlot) RecoveryBattery {
 if b==nil{return RecoveryBattery{}}
 state:=b.snapshot()
 return RecoveryBattery{CapacityWh:state.CapacityWh,EnergyWh:state.EnergyWh,Seed:state.seed,Drift:state.drift}
}
type RecoveryAttempt struct {
 ID string
 Connector int
 Generation int64
 Phase string
 TransactionID int
 StopReason string
 MeterWh int
 Battery RecoveryBattery
}
type RecoveryJournal interface {
 ReserveIntent(context.Context,int)(string,error)
 AbortIntent(context.Context,string,int)error
 BatteryInitialized(context.Context,string,int,RecoveryBattery)error
 StartIntent(context.Context,string,int)error
 StartConfirmed(context.Context,string,int,int)error
 StartRejected(context.Context,string,int)error
 RequestRemoteStopIntent(context.Context,string,int,int)error
 StopIntent(context.Context,string,int,int,core.Reason,int)error
 MeterIntent(context.Context,string,int,int,int,int,RecoveryBattery)error
 MeterConfirmed(context.Context,string,int,int)error
 StopConfirmed(context.Context,string,int,int)error
 FinishConfirmed(context.Context,string,int,int)error
}
func freshAttemptID()(string,error){
 var b [16]byte
 if _,err:=rand.Read(b[:]);err!=nil{return "",err}
 return hex.EncodeToString(b[:]),nil
}
func(s *SQLiteRecoveryStore) durable(ctx context.Context,fn func(*sql.Tx)error)error{
 s.mu.Lock();defer s.mu.Unlock()
 if s.closed||s.unhealthy{return ErrRecoveryQuarantined}
 if ctx==nil{ctx=context.Background()}
 tx,err:=s.db.BeginTx(ctx,nil)
 if err!=nil{s.unhealthy=true;return fmt.Errorf("begin recovery transaction: %w",err)}
 defer tx.Rollback()
 if err=fn(tx);err!=nil{return err}
 if s.beforeCommit!=nil {
  if err=s.beforeCommit();err!=nil{s.unhealthy=true;return fmt.Errorf("precommit failure: %w",err)}
 }
 if err=tx.Commit();err!=nil{s.unhealthy=true;return fmt.Errorf("durability failure: %w",err)}
 return nil
}
func event(ctx context.Context,tx *sql.Tx,id,phase string,csms,meter int,reason string,b RecoveryBattery)error{
 if !b.valid(){return fmt.Errorf("invalid battery state")}
 _,err:=tx.ExecContext(ctx,`INSERT INTO recovery_events(attempt_id,phase,transaction_id,meter_wh,stop_reason,capacity_wh,energy_wh,rng_seed,rng_drift)
 VALUES(?,?,?,?,?,?,?,?,?)`,id,phase,csms,meter,reason,b.CapacityWh,b.EnergyWh,strconv.FormatUint(b.Seed,10),b.Drift)
 return err
}
func(s *SQLiteRecoveryStore) ReserveIntent(ctx context.Context,connector int)(string,error){
 if connector<1||connector>s.identity.Connectors{return "",fmt.Errorf("invalid connector")}
 id,err:=freshAttemptID();if err!=nil{return "",err}
 err=s.durable(ctx,func(tx *sql.Tx)error{
  var generation int64
  if err:=tx.QueryRowContext(ctx,`SELECT generation FROM recovery_identity WHERE singleton=1 AND trust_state='Quarantined'`).Scan(&generation);err!=nil{return err}
  if _,err:=tx.ExecContext(ctx,`INSERT INTO recovery_attempts(attempt_id,connector,generation,phase) VALUES(?,?,?,'Reserved')`,id,connector,generation);err!=nil{return err}
  return event(ctx,tx,id,"Reserved",0,0,"",RecoveryBattery{})
 })
 return id,err
}
func(s *SQLiteRecoveryStore) step(ctx context.Context,id string,connector int,from []string,to string,csms,meter,watts int,reason string,battery *RecoveryBattery)error{
 if id==""||connector<1||connector>s.identity.Connectors||csms<0||meter<0||watts<0{return fmt.Errorf("invalid journal identity")}
 return s.durable(ctx,func(tx *sql.Tx)error{
  var oldPhase,oldReason,seed string
  var existing,oldMeter int
  var capacity,energy,drift float64
  var gen,live int64
  if err:=tx.QueryRowContext(ctx,`SELECT phase,transaction_id,meter_wh,stop_reason,capacity_wh,energy_wh,rng_seed,rng_drift,generation
   FROM recovery_attempts WHERE attempt_id=? AND connector=?`,id,connector).
   Scan(&oldPhase,&existing,&oldMeter,&oldReason,&capacity,&energy,&seed,&drift,&gen);err!=nil{return fmt.Errorf("unknown attempt: %w",err)}
  if err:=tx.QueryRowContext(ctx,`SELECT generation FROM recovery_identity WHERE singleton=1`).Scan(&live);err!=nil{return err}
  if gen!=live{return fmt.Errorf("stale generation %d/%d",gen,live)}
  ok:=false;for _,valid:=range from{if oldPhase==valid{ok=true}}
  if !ok{return fmt.Errorf("disallowed journal transition %s -> %s",oldPhase,to)}
  if existing>0&&existing!=csms{return fmt.Errorf("CSMS ID mismatch")}
  if csms==0&&existing>0{csms=existing}
  if meter==0&&oldMeter>0{meter=oldMeter}
  if meter<oldMeter{return fmt.Errorf("meter regression")}
  if to=="ChargingKnown"||to=="MeterIntent"||to=="StopRequested"||to=="StopIntent"||to=="StopAcked"||to=="Closed"{
   if csms<=0{return fmt.Errorf("real CSMS ID required")}
  }
  if to=="StartIntent"&&csms!=0{return fmt.Errorf("start ID cannot be fabricated")}
  if reason==""{reason=oldReason}
  b:=RecoveryBattery{CapacityWh:capacity,EnergyWh:energy,Drift:drift}
  b.Seed,_=strconv.ParseUint(seed,10,64)
  if battery!=nil{b=*battery}
  if !b.valid(){return fmt.Errorf("invalid battery transition")}
  result,err:=tx.ExecContext(ctx,`UPDATE recovery_attempts SET phase=?,transaction_id=?,meter_wh=?,stop_reason=?,power_w=?,capacity_wh=?,energy_wh=?,rng_seed=?,rng_drift=?
    WHERE attempt_id=? AND connector=? AND generation=?`,
    to,csms,meter,reason,watts,b.CapacityWh,b.EnergyWh,strconv.FormatUint(b.Seed,10),b.Drift,id,connector,gen)
  if err!=nil{return err}
  if n,err:=result.RowsAffected();err!=nil||n!=1{return fmt.Errorf("journal update failed: %v",err)}
  return event(ctx,tx,id,to,csms,meter,reason,b)
 })
}
func(s *SQLiteRecoveryStore) AbortIntent(ctx context.Context,id string,c int)error{
 return s.step(ctx,id,c,[]string{"Reserved"},"Aborted",0,0,0,"",nil)
}
func(s *SQLiteRecoveryStore) BatteryInitialized(ctx context.Context,id string,c int,b RecoveryBattery)error{
 return s.step(ctx,id,c,[]string{"Reserved"},"Reserved",0,0,0,"",&b)
}
func(s *SQLiteRecoveryStore) StartIntent(ctx context.Context,id string,c int)error{
 return s.step(ctx,id,c,[]string{"Reserved"},"StartIntent",0,0,0,"",nil)
}
func(s *SQLiteRecoveryStore) StartConfirmed(ctx context.Context,id string,c,csms int)error{
 return s.step(ctx,id,c,[]string{"StartIntent","Reserved"},"ChargingKnown",csms,0,0,"",nil)
}
func(s *SQLiteRecoveryStore) StartRejected(ctx context.Context,id string,c int)error{
 return s.step(ctx,id,c,[]string{"StartIntent"},"StartRejected",0,0,0,"",nil)
}
func(s *SQLiteRecoveryStore) RequestRemoteStopIntent(ctx context.Context,id string,c,csms int)error{
 return s.step(ctx,id,c,[]string{"ChargingKnown","MeterIntent","MeterAcked"},"StopRequested",csms,0,0,"Remote",nil)
}
func(s *SQLiteRecoveryStore) StopIntent(ctx context.Context,id string,c,csms int,reason core.Reason,meter int)error{
 return s.step(ctx,id,c,[]string{"ChargingKnown","MeterIntent","MeterAcked","StopRequested"},"StopIntent",csms,meter,0,string(reason),nil)
}
func(s *SQLiteRecoveryStore) MeterIntent(ctx context.Context,id string,c,csms,meter,watts int,b RecoveryBattery)error{
 return s.step(ctx,id,c,[]string{"ChargingKnown","MeterAcked","StopRequested"},"MeterIntent",csms,meter,watts,"",&b)
}
func(s *SQLiteRecoveryStore) MeterConfirmed(ctx context.Context,id string,c,csms int)error{
 return s.step(ctx,id,c,[]string{"MeterIntent","StopRequested"},"MeterAcked",csms,0,0,"",nil)
}
func(s *SQLiteRecoveryStore) StopConfirmed(ctx context.Context,id string,c,csms int)error{
 return s.step(ctx,id,c,[]string{"StopIntent"},"StopAcked",csms,0,0,"",nil)
}
func(s *SQLiteRecoveryStore) FinishConfirmed(ctx context.Context,id string,c,csms int)error{
 return s.step(ctx,id,c,[]string{"StopAcked"},"Closed",csms,0,0,"",nil)
}
func(s *SQLiteRecoveryStore) Attempts(ctx context.Context)([]RecoveryAttempt,error){
 s.mu.Lock();defer s.mu.Unlock()
 if s.closed||s.unhealthy{return nil,ErrRecoveryQuarantined}
 rows,err:=s.db.QueryContext(ctx,`SELECT attempt_id,connector,generation,phase,transaction_id,stop_reason,meter_wh,capacity_wh,energy_wh,rng_seed,rng_drift FROM recovery_attempts ORDER BY connector,generation,attempt_id`)
 if err!=nil{return nil,err}
 defer rows.Close()
 out:=[]RecoveryAttempt{}
 for rows.Next(){
  var a RecoveryAttempt;var seed string
  if err:=rows.Scan(&a.ID,&a.Connector,&a.Generation,&a.Phase,&a.TransactionID,&a.StopReason,&a.MeterWh,&a.Battery.CapacityWh,&a.Battery.EnergyWh,&seed,&a.Battery.Drift);err!=nil{return nil,err}
  a.Battery.Seed,err=strconv.ParseUint(seed,10,64);if err!=nil{return nil,err}
  out=append(out,a)
 }
 return out,rows.Err()
}
var _ RecoveryJournal = (*SQLiteRecoveryStore)(nil)
