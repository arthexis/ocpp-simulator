package spike

import (
 "fmt"
 "sync"
)

// ConnectorLedger maintains exclusive ownership of active transactions.
// It does not send OCPP frames or fabricate remote StopTransaction receipts.
type ConnectorLedger struct {
 mu sync.Mutex
 connectors int
 byConnector map[int]int
 byTransaction map[int]int
 pending map[int]bool
}

func NewConnectorLedger(connectors int) (*ConnectorLedger,error) {
 if connectors<1 || connectors>128 {return nil,fmt.Errorf("connectors must be between 1 and 128")}
 return &ConnectorLedger{connectors:connectors,byConnector:make(map[int]int),byTransaction:make(map[int]int),pending:make(map[int]bool)},nil
}
func (l *ConnectorLedger) Reserve(connector int) error {
 l.mu.Lock();defer l.mu.Unlock()
 if connector<1 || connector>l.connectors {return fmt.Errorf("invalid connector %d",connector)}
 if l.pending[connector] || l.byConnector[connector]!=0 {return fmt.Errorf("connector %d occupied",connector)}
 l.pending[connector]=true
 return nil
}
func (l *ConnectorLedger) Start(connector,transactionID int) error {
 l.mu.Lock();defer l.mu.Unlock()
 if !l.pending[connector] || transactionID<=0 || l.byTransaction[transactionID]!=0 {
  return fmt.Errorf("unreserved connector or duplicate transaction")
 }
 delete(l.pending,connector)
 l.byConnector[connector]=transactionID
 l.byTransaction[transactionID]=connector
 return nil
}
func (l *ConnectorLedger) Abort(connector int) {
 l.mu.Lock();defer l.mu.Unlock()
 delete(l.pending,connector)
}
func (l *ConnectorLedger) ConnectorFor(transactionID int) (int,bool) {
 l.mu.Lock();defer l.mu.Unlock()
 c,ok:=l.byTransaction[transactionID]
 return c,ok
}
func (l *ConnectorLedger) Finish(connector,transactionID int) error {
 l.mu.Lock();defer l.mu.Unlock()
 if transactionID<=0 || l.byConnector[connector]!=transactionID || l.byTransaction[transactionID]!=connector {
  return fmt.Errorf("transaction does not belong to connector")
 }
 delete(l.byConnector,connector)
 delete(l.byTransaction,transactionID)
 return nil
}
