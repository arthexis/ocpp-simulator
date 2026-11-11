package spike

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func r6c1Identity() RecoveryIdentity {
	return RecoveryIdentity{
		Tenant: "isolated-fixture",
		CP: "R6-C1",
		CSMS: "ws://127.0.0.1:18000/ocpp/R6-C1",
		Connectors: 2,
	}
}

func TestR6C1PrivateSQLiteQuarantineGenerationAndRecovery(t *testing.T) {
	root := t.TempDir()
	id := r6c1Identity()
	ctx := context.Background()
	first, err := OpenSQLiteRecoveryStore(root, id)
	if err != nil { t.Fatal(err) }
	status, err := first.Status(ctx)
	if err != nil || status.TrustState != "Quarantined" || status.Generation <= 0 {
		t.Fatalf("initial store not quarantined: %+v %v", status, err)
	}
	if !errors.Is(first.BeforeAvailable(ctx), ErrRecoveryQuarantined) {
		t.Fatal("new SQLite DB incorrectly allowed initial Available")
	}
	if err := first.RecordUncertain(ctx, RecoveryPending{Connector:1,State:SessionStarting}); err != nil { t.Fatal(err) }
	if err := first.RecordUncertain(ctx, RecoveryPending{Connector:2,State:SessionStopping,TransactionID:9902}); err != nil { t.Fatal(err) }
	if err := first.Close(); err != nil { t.Fatal(err) }
	second, err := OpenSQLiteRecoveryStore(root, id)
	if err != nil { t.Fatal(err) }
	defer second.Close()
	after, err := second.Status(ctx)
	if err != nil { t.Fatal(err) }
	if after.Generation != status.Generation+1 || len(after.Pending) != 2 ||
		after.Pending[0].Connector != 1 || after.Pending[0].State != SessionStarting ||
		after.Pending[0].TransactionID != 0 || after.Pending[1].Connector != 2 ||
		after.Pending[1].State != SessionStopping || after.Pending[1].TransactionID != 9902 {
		t.Fatalf("loss of durable uncertain state or generation: %+v", after)
	}
	if !errors.Is(second.BeforeAvailable(ctx), ErrRecoveryQuarantined) {
		t.Fatal("restart promoted unverified state to Available")
	}
	entries, err := os.ReadDir(root)
	if err != nil { t.Fatal(err) }
	foundSQLite := false
	for _, ent := range entries {
		info, err := os.Lstat(filepath.Join(root, ent.Name()))
		if err != nil { t.Fatal(err) }
		if info.Mode().Perm() != 0600 || !info.Mode().IsRegular() {
			t.Fatalf("non-private SQLite/lock/sidecar file %s %s",ent.Name(),info.Mode())
		}
		if strings.HasSuffix(ent.Name(), ".sqlite") {foundSQLite = true}
	}
	if !foundSQLite {t.Fatal("SQLite DB not created in private root")}
	if info, err := os.Stat(root);err != nil || info.Mode().Perm()!=0700 {
		t.Fatal("recovery root not private",err)
	}
}

func TestR6C1MissingIdentityAndUntrustedStore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "new-private-root")
	id := r6c1Identity()
	for _,bad:=range []RecoveryIdentity{
		{}, {Tenant:"x",CP:"../bad",CSMS:id.CSMS,Connectors:2},
		{Tenant:"../bad",CP:id.CP,CSMS:id.CSMS,Connectors:2},
		{Tenant:id.Tenant,CP:id.CP,CSMS:"ws://user:pass@127.0.0.1/ocpp/R6-C1",Connectors:2},
		{Tenant:id.Tenant,CP:id.CP,CSMS:id.CSMS,Connectors:129},
	}{
		if _,err:=OpenSQLiteRecoveryStore(root,bad);err==nil {t.Fatalf("invalid identity accepted: %+v",bad)}
	}
	store,err:=OpenSQLiteRecoveryStore(root,id)
	if err!=nil {t.Fatal(err)}
	if !errors.Is(store.BeforeAvailable(context.Background()),ErrRecoveryQuarantined){t.Fatal("missing DB was promoted clean")}
	if err:=store.Close();err!=nil {t.Fatal(err)}
	// A different connector count maps to the SAME identity DB but may not
	// silently reinterpret connector ownership.
	changed:=id
	changed.Connectors=1
	if _,err:=OpenSQLiteRecoveryStore(root,changed);err==nil {
		t.Fatal("connector-count drift opened an incompatible recovery database")
	}
	// A changed tenant or server endpoint maps to a different DB identity.
	// It must not inherit a previous generation or assume a fresh CP is free.
	for _,changed:=range []RecoveryIdentity{
		func()RecoveryIdentity{v:=id;v.Tenant="another";return v}(),
		func()RecoveryIdentity{v:=id;v.CSMS="wss://csms.example.test/ocpp/R6-C1";return v}(),
	}{
		s,err:=OpenSQLiteRecoveryStore(root,changed)
		if err!=nil {t.Fatal(err)}
		st,err:=s.Status(context.Background())
		if err!=nil || len(st.Pending)!=0 || st.TrustState!="Quarantined" {t.Fatal(st,err)}
		if !errors.Is(s.BeforeAvailable(context.Background()),ErrRecoveryQuarantined){t.Fatal("changed identity trusted")}
		_ = s.Close()
	}
}

func TestR6C1LockSurvivesAcrossProcesses(t *testing.T) {
	if os.Getenv("R6C1_HELPER") == "1" {
		id:=r6c1Identity()
		s,err:=OpenSQLiteRecoveryStore(os.Getenv("R6C1_ROOT"),id)
		if err!=nil {os.Exit(17)}
		_ = s.Close()
		os.Exit(0)
	}
	root:=t.TempDir()
	first,err:=OpenSQLiteRecoveryStore(root,r6c1Identity())
	if err!=nil {t.Fatal(err)}
	runHelper:=func() error {
		ctx,cancel:=context.WithTimeout(context.Background(),4*time.Second)
		defer cancel()
		cmd:=exec.CommandContext(ctx,os.Args[0],"-test.run=^TestR6C1LockSurvivesAcrossProcesses$")
		cmd.Env=append(os.Environ(),"R6C1_HELPER=1","R6C1_ROOT="+root)
		return cmd.Run()
	}
	if err:=runHelper();err==nil{t.Fatal("second process stole live CP recovery state lock")}
	if err:=first.Close();err!=nil{t.Fatal(err)}
	if err:=runHelper();err!=nil{t.Fatalf("released lock still denies next owner: %v",err)}
}

func TestR6C1CorruptUnsafePermissionsAndSymlinkFailClosed(t *testing.T) {
	id:=r6c1Identity()
	root:=t.TempDir()
	s,err:=OpenSQLiteRecoveryStore(root,id)
	if err!=nil {t.Fatal(err)}
	key:=s.hash
	_ = s.Close()
	dbPath:=filepath.Join(root,key+".sqlite")
	if err:=os.Chmod(dbPath,0644);err!=nil{t.Fatal(err)}
	if _,err:=OpenSQLiteRecoveryStore(root,id);err==nil{t.Fatal("accepted world-readable recovery DB")}
	if err:=os.Chmod(dbPath,0600);err!=nil{t.Fatal(err)}
	if err:=os.WriteFile(dbPath,[]byte("not sqlite, no valid pages"),0600);err!=nil{t.Fatal(err)}
	if _,err:=OpenSQLiteRecoveryStore(root,id);err==nil{t.Fatal("accepted corrupt SQLite DB")}
	symlinkRoot:=filepath.Join(t.TempDir(),"state-symlink")
	if err:=os.Symlink(root,symlinkRoot);err!=nil{t.Fatal(err)}
	if _,err:=OpenSQLiteRecoveryStore(symlinkRoot,id);err==nil{t.Fatal("followed symlinked recovery directory")}
	if _,err:=OpenSQLiteRecoveryStore("relative/unsafe",id);err==nil{t.Fatal("accepted relative recovery root")}
}

func TestR6C1FsyncAndWriteFailureInjectionNeverUnlock(t *testing.T) {
	root:=t.TempDir()
	id:=r6c1Identity()
	_,err:=openSQLiteRecoveryStore(root,id,func(string)error{return errors.New("EIO fsync")})
	if err==nil || !strings.Contains(err.Error(),"fsync"){t.Fatalf("injected fsync error accepted: %v",err)}
	s,err:=OpenSQLiteRecoveryStore(root,id)
	if err!=nil{t.Fatal(err)}
	defer s.Close()
	s.beforeCommit=func()error{return errors.New("ENOSPC injected before durable commit")}
	err=s.RecordUncertain(context.Background(),RecoveryPending{Connector:1,State:SessionStarting})
	if err==nil || !strings.Contains(err.Error(),"ENOSPC"){t.Fatalf("accepted failed write: %v",err)}
	if _,err:=s.Status(context.Background());!errors.Is(err,ErrRecoveryQuarantined){
		t.Fatalf("failed persistence did not poison store: %v",err)
	}
	if !errors.Is(s.BeforeAvailable(context.Background()),ErrRecoveryQuarantined) {
		t.Fatal("persistence failure unlocked remote service")
	}
	_ = s.Close()
	second,err:=OpenSQLiteRecoveryStore(root,id)
	if err!=nil{t.Fatal(err)}
	defer second.Close()
	status,err:=second.Status(context.Background())
	if err!=nil || len(status.Pending)!=0 || status.TrustState!="Quarantined" {
		t.Fatalf("rolled-back write or stored state unexpected: %+v %v",status,err)
	}
}

func TestR6C1TransactionalConstraintRejectsConflictingTransaction(t *testing.T) {
	s,err:=OpenSQLiteRecoveryStore(t.TempDir(),r6c1Identity())
	if err!=nil{t.Fatal(err)}
	defer s.Close()
	ctx:=context.Background()
	if err:=s.RecordUncertain(ctx,RecoveryPending{Connector:1,State:SessionStarting});err!=nil{t.Fatal(err)}
	if err:=s.RecordUncertain(ctx,RecoveryPending{Connector:1,State:SessionCharging,TransactionID:775});err==nil{
		t.Fatal("overwrote unresolved start with a different server ID without reconciliation")
	}
	if !errors.Is(s.BeforeAvailable(ctx),ErrRecoveryQuarantined){t.Fatal("conflict unlocked gate")}
}

func TestR6C1RemoteServeDeniedBeforeOpeningWebSocket(t *testing.T) {
	id:=r6c1Identity()
	o:=ServeOptions{
		Options:Options{CP:id.CP,URL:id.CSMS,Vendor:"Arthexis",Model:"Simulator"},
		ConnectTimeout:time.Second,Remote:true,Connectors:id.Connectors,
		RecoveryRoot:t.TempDir(),RecoveryTenant:id.Tenant,
	}
	if err:=Serve(context.Background(),o);!errors.Is(err,ErrRecoveryQuarantined){
		t.Fatalf("real entrypoint did not deny unreconciled CSMS: %v",err)
	}
	if err:=ValidateServe(ServeOptions{Options:o.Options,ConnectTimeout:time.Second,Remote:true,Connectors:2});err==nil{
		t.Fatal("remote mode without recovery identity/directory accepted")
	}
	if err:=ValidateServe(ServeOptions{Options:o.Options,ConnectTimeout:time.Second,RecoveryRoot:o.RecoveryRoot});err==nil{
		t.Fatal("non-remote ignored recovery options")
	}
	// An external user or CLI can never opt into the unexported test-only
	// loopback fixture; it is also disabled in built binaries by testing.Testing.
	unsafe:=o
	unsafe.testOnlyLoopback=true
	unsafe.Options.URL="wss://example.test/R6-C1"
	if err:=ValidateServe(unsafe);err==nil{t.Fatal("test-only authority accepted an external CSMS")}
}

func TestR6C1RecoveryGateDoesNotClaimCleanVerification(t *testing.T) {
	var gate RecoveryGate
	store,err:=OpenSQLiteRecoveryStore(t.TempDir(),r6c1Identity())
	if err!=nil{t.Fatal(err)}
	defer store.Close()
	gate=store
	if err:=gate.BeforeAvailable(context.Background());!errors.Is(err,ErrRecoveryQuarantined){
		t.Fatal(fmt.Errorf("local SQLite was incorrectly accepted as CSMS authority: %v",err))
	}
}
