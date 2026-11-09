package spike

import (
 "sync"
 "testing"
)
func TestConnectorLedgerOwnershipAndRouting(t *testing.T) {
 l,err:=NewConnectorLedger(2)
 if err!=nil {t.Fatal(err)}
 if err:=l.Reserve(1);err!=nil {t.Fatal(err)}
 if err:=l.Reserve(1);err==nil {t.Fatal("duplicate reservation")}
 if err:=l.Start(1,41);err!=nil {t.Fatal(err)}
 if err:=l.Reserve(1);err==nil {t.Fatal("occupied connector")}
 if err:=l.Reserve(2);err!=nil {t.Fatal(err)}
 if err:=l.Start(2,41);err==nil {t.Fatal("duplicate transaction ID")}
 if err:=l.Start(2,42);err!=nil {t.Fatal(err)}
 if c,ok:=l.ConnectorFor(42);!ok||c!=2 {t.Fatal("wrong routing")}
 if err:=l.Finish(1,42);err==nil {t.Fatal("cross-connector stop")}
 if err:=l.Finish(2,42);err!=nil {t.Fatal(err)}
 if _,ok:=l.ConnectorFor(42);ok {t.Fatal("stale transaction")}
 if err:=l.Finish(2,42);err==nil {t.Fatal("duplicate finish")}
 if err:=l.Reserve(2);err!=nil {t.Fatal(err)}
 l.Abort(2)
 if err:=l.Reserve(2);err!=nil {t.Fatal("abort did not release reservation")}
}
func TestConnectorLedgerInvalidAndConcurrent(t *testing.T) {
 for _,n:=range []int{0,-1,129} {if _,err:=NewConnectorLedger(n);err==nil {t.Fatalf("accepted %d",n)}}
 l,_:=NewConnectorLedger(1)
 for _,n:=range []int{0,-1,2} {if err:=l.Reserve(n);err==nil {t.Fatalf("accepted connector %d",n)}}
 var wg sync.WaitGroup
 successes:=make(chan bool,64)
 for i:=0;i<64;i++ {wg.Add(1);go func(){defer wg.Done();successes<-l.Reserve(1)==nil}()}
 wg.Wait();close(successes)
 count:=0
 for ok:=range successes {if ok {count++}}
 if count!=1 {t.Fatalf("expected exactly one owner, got %d",count)}
 if err:=l.Start(1,0);err==nil {t.Fatal("accepted invalid transaction ID")}
 l.Abort(1)
 if err:=l.Reserve(1);err!=nil {t.Fatal(err)}
}
