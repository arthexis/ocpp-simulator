package spike

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

// R6C-1 is intentionally a one-way quarantine gate. No production CSMS
// reconciliation authority has been approved, so CleanVerified cannot be
// created locally and no saved DB state grants permission to start charging.
var ErrRecoveryQuarantined = errors.New("recovery is quarantined: authoritative CSMS reconciliation is required")

const recoverySchemaVersion = 2

type RecoveryIdentity struct {
	Tenant     string
	CSMS       string // canonical CSMS endpoint, never userinfo or credentials
	CP         string
	Connectors int
}

func (i RecoveryIdentity) canonical() (RecoveryIdentity, string, error) {
	if !cpName.MatchString(i.CP) || i.CP == "." || i.CP == ".." ||
		!cpName.MatchString(i.Tenant) || i.Tenant == "." || i.Tenant == ".." ||
		i.Connectors < 1 || i.Connectors > 128 {
		return i, "", fmt.Errorf("invalid recovery identity, tenant or connector count")
	}
	u, err := url.Parse(i.CSMS)
	if err != nil || u == nil || (u.Scheme != "ws" && u.Scheme != "wss") ||
		u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		!strings.HasSuffix(u.Path, "/"+i.CP) {
		return i, "", fmt.Errorf("invalid CSMS recovery endpoint")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.RawPath = ""
	u.Path = strings.TrimSuffix(u.Path, "/"+i.CP)
	u.RawQuery, u.Fragment = "", ""
	i.CSMS = u.String()
	raw := i.Tenant + "\x00" + i.CSMS + "\x00" + i.CP + "\x00ocpp1.6j"
	hash := sha256.Sum256([]byte(raw))
	return i, hex.EncodeToString(hash[:]), nil
}

type RecoveryStatus struct {
	Identity   RecoveryIdentity
	Generation int64
	TrustState string
	Pending    []RecoveryPending
}

type RecoveryPending struct {
	Connector     int
	State         SessionState
	TransactionID int // zero means an unknown StartTransaction ID
}

// RecoveryGate is the security boundary: an empty or valid local database is
// not an authority to emit Available. R6C-1 has no implementation capable of
// returning CleanVerified; future work needs a separately approved CSMS
// witness, including its generation, fencing token and snapshot revision.
type RecoveryGate interface {
	BeforeAvailable(context.Context) error
}

type RecoveryStorage interface {
	RecoveryGate
	Status(context.Context) (RecoveryStatus, error)
	RecordUncertain(context.Context, RecoveryPending) error
	Close() error
}

type SQLiteRecoveryStore struct {
	mu       sync.Mutex
	db       *sql.DB
	lock     *os.File
	identity RecoveryIdentity
	hash     string
	closed   bool
	unhealthy bool
	beforeCommit func() error // package-private fault injection; never exposed by CLI
}

func privateRecoveryRoot(root string) error {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return fmt.Errorf("recovery root must be an explicit canonical absolute path")
	}
	// Reject symlink components, including parents; never follow an
	// attacker-controlled directory redirect to an existing SQLite file.
	vol := filepath.VolumeName(root)
	cur := string(os.PathSeparator)
	if vol != "" {
		cur = vol + string(os.PathSeparator)
	}
	part := strings.TrimPrefix(strings.TrimPrefix(root, vol), string(os.PathSeparator))
	for _, component := range strings.Split(part, string(os.PathSeparator)) {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("unsafe recovery path component")
		}
		cur = filepath.Join(cur, component)
		fi, err := os.Lstat(cur)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(cur, 0700); err != nil { return fmt.Errorf("create private recovery directory: %w", err) }
			fi, err = os.Lstat(cur)
		}
		if err != nil { return fmt.Errorf("inspect recovery path: %w", err) }
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("recovery path contains non-directory or symlink")
		}
	}
	fi, err := os.Lstat(root)
	if err != nil { return err }
	if fi.Mode().Perm() != 0700 {
		return fmt.Errorf("recovery directory must have exact permissions 0700")
	}
	return nil
}

func secureRecoveryFile(path string) error {
	fi, err := os.Lstat(path)
	if err != nil { return err }
	if !fi.Mode().IsRegular() || fi.Mode().Perm() != 0600 || fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("unsafe recovery file permissions or file type")
	}
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok && stat.Nlink != 1 {
		return fmt.Errorf("recovery file must not have hardlinks")
	}
	return nil
}

func syncRecoveryDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil { return err }
	defer f.Close()
	return f.Sync()
}

func secureOpenRecoveryFile(path string) (*os.File, error) {
	flags := syscall.O_RDWR | syscall.O_CREAT | syscall.O_NOFOLLOW | syscall.O_CLOEXEC
	fd, err := syscall.Open(path, flags, 0600)
	if err != nil { return nil, err }
	f := os.NewFile(uintptr(fd), path)
	if err := secureRecoveryFile(path); err != nil { _ = f.Close(); return nil, err }
	return f, nil
}

// OpenSQLiteRecoveryStore creates only private, identity-scoped recovery
// metadata. Every open advances generation and durably records Quarantined.
// The lock is held for the lifetime of the store. The API cannot grant
// CleanVerified, replay a transaction or contact a CSMS.
func OpenSQLiteRecoveryStore(root string, identity RecoveryIdentity) (*SQLiteRecoveryStore, error) {
	return openSQLiteRecoveryStore(root, identity, syncRecoveryDir)
}

// syncFn is injectable only within this internal package for offline fsync
// fault tests; the public entrypoint always uses the real directory fsync.
func openSQLiteRecoveryStore(root string, identity RecoveryIdentity, syncFn func(string) error) (store *SQLiteRecoveryStore, err error) {
	identity, key, err := identity.canonical()
	if err != nil { return nil, err }
	if err := privateRecoveryRoot(root); err != nil { return nil, err }
	store = &SQLiteRecoveryStore{identity:identity, hash:key}
	handle := store
	defer func() { if err != nil { _ = handle.Close(); store = nil } }()

	lockPath := filepath.Join(root, key+".lock")
	lock, openErr := secureOpenRecoveryFile(lockPath)
	if openErr != nil { return nil, fmt.Errorf("open recovery lock: %w", openErr) }
	store.lock = lock
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("recovery identity already owned or lock unavailable: %w", err)
	}
	dbPath := filepath.Join(root, key+".sqlite")
	newFile := false
	if _, err := os.Lstat(dbPath); errors.Is(err, os.ErrNotExist) {
		created, createErr := syscall.Open(dbPath, syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
		if createErr != nil && !errors.Is(createErr, syscall.EEXIST) { return nil, createErr }
		if createErr == nil {
			_ = syscall.Close(created)
			newFile = true
		}
	} else if err != nil {
		return nil, fmt.Errorf("inspect recovery database: %w", err)
	}
	if err := secureRecoveryFile(dbPath); err != nil { return nil, err }
	if newFile {
		if err := syncFn(root); err != nil { return nil, fmt.Errorf("sync new recovery DB directory: %w", err) }
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil { return nil, fmt.Errorf("open sqlite recovery: %w", err) }
	store.db = db
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil { return nil, fmt.Errorf("sqlite unavailable: %w", err) }
	var mode string
	if err := db.QueryRowContext(ctx,"PRAGMA journal_mode=WAL").Scan(&mode); err != nil || !strings.EqualFold(mode,"wal") {
		return nil, fmt.Errorf("sqlite WAL required: mode=%q: %w", mode, err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil { return nil, err }
	var syncLevel int
	if err := db.QueryRowContext(ctx,"PRAGMA synchronous").Scan(&syncLevel); err != nil || syncLevel != 2 {
		return nil, fmt.Errorf("sqlite synchronous FULL required: %d: %w", syncLevel, err)
	}
	var integrity string
	if err := db.QueryRowContext(ctx,"PRAGMA quick_check").Scan(&integrity); err != nil || integrity != "ok" {
		return nil, fmt.Errorf("sqlite recovery database corrupted: %q: %w", integrity, err)
	}
	var version int
	if err := db.QueryRowContext(ctx,"PRAGMA user_version").Scan(&version); err != nil { return nil, err }
	if version != 0 && version != recoverySchemaVersion { return nil, fmt.Errorf("unknown recovery schema version %d", version) }
	tx, err := db.BeginTx(ctx,nil)
	if err != nil { return nil, err }
	defer tx.Rollback()
	if version == 0 {
		if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS recovery_identity(
			singleton INTEGER PRIMARY KEY CHECK(singleton=1),
			identity_hash TEXT NOT NULL,
			tenant TEXT NOT NULL,
			csms TEXT NOT NULL,
			cp TEXT NOT NULL,
			connectors INTEGER NOT NULL CHECK(connectors BETWEEN 1 AND 128),
			generation INTEGER NOT NULL CHECK(generation>=1),
			trust_state TEXT NOT NULL CHECK(trust_state='Quarantined'))`); err != nil { return nil, err }
		if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS recovery_pending(
			connector INTEGER PRIMARY KEY CHECK(connector BETWEEN 1 AND 128),
			state TEXT NOT NULL CHECK(state IN ('Reserved','Starting','Charging','Stopping')),
			transaction_id INTEGER NOT NULL DEFAULT 0 CHECK(transaction_id>=0))`); err != nil { return nil, err }
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO recovery_identity
			(singleton,identity_hash,tenant,csms,cp,connectors,generation,trust_state)
			VALUES(1,?,?,?,?,?,0+1,'Quarantined')`,
			key, identity.Tenant, identity.CSMS, identity.CP, identity.Connectors); err != nil { return nil, err }
	}
	// Upgrade only the private Go recovery database, never ocpp-csms SQLite.
	// Existing R6C-1 uncertainty remains in recovery_pending, quarantined.
	if version < 2 {
		if _, err = tx.ExecContext(ctx, `CREATE TABLE recovery_attempts(
			attempt_id TEXT PRIMARY KEY,
			connector INTEGER NOT NULL CHECK(connector BETWEEN 1 AND 128),
			generation INTEGER NOT NULL CHECK(generation>0),
			phase TEXT NOT NULL,
			transaction_id INTEGER NOT NULL DEFAULT 0 CHECK(transaction_id>=0),
			stop_reason TEXT NOT NULL DEFAULT '',
			meter_wh INTEGER NOT NULL DEFAULT 0 CHECK(meter_wh>=0),
			power_w INTEGER NOT NULL DEFAULT 0 CHECK(power_w>=0),
			capacity_wh REAL NOT NULL DEFAULT 0,
			energy_wh REAL NOT NULL DEFAULT 0,
			rng_seed TEXT NOT NULL DEFAULT '0',
			rng_drift REAL NOT NULL DEFAULT 0)`); err != nil { return nil,err }
		if _,err=tx.ExecContext(ctx,`CREATE UNIQUE INDEX active_connector_owner ON recovery_attempts(connector)
		  WHERE phase NOT IN ('Aborted','StartRejected','Closed')`);err!=nil{return nil,err}
		if _,err=tx.ExecContext(ctx,`CREATE UNIQUE INDEX active_transaction_owner ON recovery_attempts(transaction_id)
		  WHERE transaction_id>0 AND phase NOT IN ('Aborted','StartRejected','Closed')`);err!=nil{return nil,err}
		if _,err=tx.ExecContext(ctx,`CREATE TABLE recovery_events(
		  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
		  attempt_id TEXT NOT NULL REFERENCES recovery_attempts(attempt_id),
		  phase TEXT NOT NULL,
		  transaction_id INTEGER NOT NULL,
		  meter_wh INTEGER NOT NULL,
		  stop_reason TEXT NOT NULL,
		  capacity_wh REAL NOT NULL,
		  energy_wh REAL NOT NULL,
		  rng_seed TEXT NOT NULL,
		  rng_drift REAL NOT NULL)`);err!=nil{return nil,err}
		if _,err=tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version=%d",recoverySchemaVersion));err!=nil{return nil,err}
	}
	var storedHash, tenant, endpoint, cp, trust string
	var connectors int
	if err = tx.QueryRowContext(ctx, `SELECT identity_hash,tenant,csms,cp,connectors,trust_state
		FROM recovery_identity WHERE singleton=1`).Scan(&storedHash,&tenant,&endpoint,&cp,&connectors,&trust); err != nil { return nil, fmt.Errorf("missing durable recovery identity: %w",err) }
	if storedHash != key || tenant != identity.Tenant || endpoint != identity.CSMS || cp != identity.CP ||
		connectors != identity.Connectors || trust != "Quarantined" {
		return nil, fmt.Errorf("recovery identity/schema/trust mismatch: quarantine")
	}
	if _, err = tx.ExecContext(ctx,`UPDATE recovery_identity SET generation=generation+1,trust_state='Quarantined' WHERE singleton=1`); err != nil { return nil, err }
	if err = tx.Commit(); err != nil { return nil, fmt.Errorf("sqlite quarantine commit: %w",err) }
	if err = syncFn(root); err != nil { return nil, fmt.Errorf("sync recovery directory: %w",err) }
	for _, suffix := range []string{"","-wal","-shm"} {
		path:=dbPath+suffix
		if err:=secureRecoveryFile(path); err!=nil && !errors.Is(err,os.ErrNotExist) {
			return nil,fmt.Errorf("unsafe sqlite sidecar: %w",err)
		}
	}
	return store,nil
}

func (s *SQLiteRecoveryStore) BeforeAvailable(context.Context) error {
	// Not even a syntactically correct, empty database grants CSMS authority.
	return ErrRecoveryQuarantined
}

func (s *SQLiteRecoveryStore) Status(ctx context.Context) (RecoveryStatus,error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.unhealthy { return RecoveryStatus{},ErrRecoveryQuarantined }
	var generation int64
	var trust string
	if err:=s.db.QueryRowContext(ctx,`SELECT generation,trust_state FROM recovery_identity WHERE singleton=1`).Scan(&generation,&trust); err!=nil {
		s.unhealthy=true
		return RecoveryStatus{},fmt.Errorf("read recovery state: %w",err)
	}
	if generation<=0 || trust!="Quarantined" { s.unhealthy=true;return RecoveryStatus{},ErrRecoveryQuarantined }
	rows,err:=s.db.QueryContext(ctx,`SELECT connector,state,transaction_id FROM recovery_pending ORDER BY connector`)
	if err!=nil {s.unhealthy=true;return RecoveryStatus{},err}
	defer rows.Close()
	out:=RecoveryStatus{Identity:s.identity,Generation:generation,TrustState:trust}
	for rows.Next() {
		var p RecoveryPending
		if err:=rows.Scan(&p.Connector,&p.State,&p.TransactionID);err!=nil{s.unhealthy=true;return RecoveryStatus{},err}
		if p.Connector<1||p.Connector>s.identity.Connectors{s.unhealthy=true;return RecoveryStatus{},ErrRecoveryQuarantined}
		out.Pending=append(out.Pending,p)
	}
	if err:=rows.Err();err!=nil{s.unhealthy=true;return RecoveryStatus{},err}
	return out,nil
}

func (s *SQLiteRecoveryStore) RecordUncertain(ctx context.Context, p RecoveryPending) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.unhealthy {return ErrRecoveryQuarantined}
	if p.Connector<1||p.Connector>s.identity.Connectors||p.TransactionID<0||
		(p.State!=SessionReserved&&p.State!=SessionStarting&&p.State!=SessionCharging&&p.State!=SessionStopping) {
		return fmt.Errorf("invalid uncertain recovery record")
	}
	tx,err:=s.db.BeginTx(ctx,nil)
	if err!=nil{s.unhealthy=true;return err}
	defer tx.Rollback()
	res,execErr:=tx.ExecContext(ctx,`INSERT INTO recovery_pending(connector,state,transaction_id)
		VALUES(?,?,?) ON CONFLICT(connector) DO UPDATE SET state=excluded.state,transaction_id=excluded.transaction_id
		WHERE recovery_pending.transaction_id=excluded.transaction_id`,
		p.Connector,p.State,p.TransactionID)
	if execErr!=nil{s.unhealthy=true;return execErr}
	if n,err:=res.RowsAffected();err!=nil||n!=1{s.unhealthy=true;return fmt.Errorf("conflicting recovery transaction identity: %v",err)}
	if s.beforeCommit!=nil {
		if err=s.beforeCommit();err!=nil{s.unhealthy=true;return fmt.Errorf("recovery persistence failed before commit: %w",err)}
	}
	if err=tx.Commit();err!=nil{s.unhealthy=true;return fmt.Errorf("recovery commit failed: %w",err)}
	return nil
}

func (s *SQLiteRecoveryStore) Close() error {
	if s==nil { return nil }
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {return nil}
	s.closed=true
	var first error
	if s.db!=nil {if err:=s.db.Close();err!=nil{first=err}}
	if s.lock!=nil {
		_ = syscall.Flock(int(s.lock.Fd()),syscall.LOCK_UN)
		if err:=s.lock.Close();first==nil{first=err}
	}
	return first
}
