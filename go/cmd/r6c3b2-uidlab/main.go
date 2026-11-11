// R6C-3B2-LAB-6: isolated test executable. Not imported by the simulator.
// Never run on the host: run() requires a private loopback-only network
// namespace and a marker in the sandbox's otherwise hidden /lab tree.
package main

import (
    "bytes"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "net"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "time"

    "github.com/lorenzodonini/ocpp-go/ocpp"
    "github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
    "github.com/lorenzodonini/ocpp-go/ocpp1.6/types"
    "github.com/lorenzodonini/ocpp-go/ocppj"
    "github.com/lorenzodonini/ocpp-go/ws"
)

var errQuarantined = errors.New("R6C-3B2-LAB-6: quarantined, no admission")
const labEndpoint = "ws://127.0.0.1:39291"
const labCP = "lab-cp-uid"

type intent struct {
    SourceID string
    DatabaseIncarnation string
    Tenant string
    CP string
    Epoch int64
    AttemptID string
    UID string
    Action string
}
func newIntent(attempt, uid string) intent {
    return intent{"r6c3b2-lab", "r6c3b2-db", "r6c3b2-tenant", labCP, 1, attempt, uid, core.StartTransactionFeatureName}
}
func safeToken(s string, max int) bool {
    return s != "" && len(s) <= max && strings.TrimSpace(s) == s &&
        !strings.ContainsAny(s, "/\\\r\n\x00")
}
func validIntent(i intent) bool {
    return i.SourceID == "r6c3b2-lab" && i.DatabaseIncarnation == "r6c3b2-db" &&
        i.Tenant == "r6c3b2-tenant" && i.CP == labCP && i.Epoch == 1 &&
        i.Action == core.StartTransactionFeatureName && safeToken(i.AttemptID, 80) && safeToken(i.UID, 36)
}
func bindingName(prefix, token string) string {
    sum := sha256.Sum256([]byte(token))
    return prefix + "-" + hex.EncodeToString(sum[:]) + ".json"
}
func reserve(dir string, i intent) error {
    if !validIntent(i) {
        return errQuarantined
    }
    st, err := os.Lstat(dir)
    if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm() != 0700 {
        return fmt.Errorf("%w: journal dir must be existing 0700", errQuarantined)
    }
    raw, err := json.Marshal(i)
    if err != nil { return errQuarantined }
    raw = append(raw, '\n')
    // Any interrupted partial record remains uncertain. Never delete/reuse it.
    for _, name := range []string{bindingName("attempt", i.AttemptID), bindingName("uid", i.UID)} {
        file, e := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
        if e != nil { return fmt.Errorf("%w: duplicate/unavailable reservation", errQuarantined) }
        _, e = file.Write(raw)
        if e == nil { e = file.Sync() }
        ce := file.Close()
        if e != nil || ce != nil { return fmt.Errorf("%w: incomplete fsync", errQuarantined) }
    }
    d, err := os.Open(dir)
    if err != nil { return errQuarantined }
    e := d.Sync()
    ce := d.Close()
    if e != nil || ce != nil { return fmt.Errorf("%w: directory fsync", errQuarantined) }
    return nil
}
type binding struct {
    identity intent
    serialized []byte
}
type uidDispatcher struct {
    ocppj.ClientDispatcher
    journal string
    mu sync.Mutex
    pending map[*core.StartTransactionRequest]binding
}
func newUIDDispatcher(dir string, underlying ocppj.ClientDispatcher) *uidDispatcher {
    return &uidDispatcher{ClientDispatcher: underlying, journal:dir, pending:make(map[*core.StartTransactionRequest]binding)}
}
func (d *uidDispatcher) bind(req *core.StartTransactionRequest, i intent) error {
    if req == nil || req.ConnectorId != 1 || req.IdTag != "lab-rfid" ||
        req.MeterStart != 0 || req.Timestamp == nil { return errQuarantined }
    i.UID = "pending-bind"
    if !validIntent(i) { return errQuarantined }
    raw, err := json.Marshal(req)
    if err != nil { return errQuarantined }
    d.mu.Lock()
    defer d.mu.Unlock()
    if _, exists := d.pending[req]; exists { return errQuarantined }
    d.pending[req] = binding{i, raw}
    return nil
}
func (d *uidDispatcher) SendRequest(b ocppj.RequestBundle) error {
    if b.Call == nil || b.Call.Action != core.StartTransactionFeatureName { return errQuarantined }
    req, ok := b.Call.Payload.(*core.StartTransactionRequest)
    if !ok || req == nil { return errQuarantined }
    d.mu.Lock()
    bound, found := d.pending[req]
    delete(d.pending, req) // one-shot, including errors
    d.mu.Unlock()
    if !found { return errQuarantined }
    current, err := json.Marshal(req)
    if err != nil || !bytes.Equal(current, bound.serialized) { return errQuarantined }
    var frame []json.RawMessage
    if json.Unmarshal(b.Data, &frame) != nil || len(frame) != 4 { return errQuarantined }
    var kind int
    var uid, action string
    if json.Unmarshal(frame[0], &kind) != nil || json.Unmarshal(frame[1], &uid) != nil ||
       json.Unmarshal(frame[2], &action) != nil || kind != 2 || uid != b.Call.UniqueId ||
       action != core.StartTransactionFeatureName { return errQuarantined }
    var wire, currentPayload any
    if json.Unmarshal(frame[3], &wire) != nil || json.Unmarshal(current, &currentPayload) != nil {
        return errQuarantined
    }
    left, _ := json.Marshal(wire)
    right, _ := json.Marshal(currentPayload)
    if !bytes.Equal(left, right) { return errQuarantined }
    bound.identity.UID = uid
    if err := reserve(d.journal, bound.identity); err != nil { return err }
    // Durable attempt/UID and directory fsync complete BEFORE enqueue/send.
    if err := d.ClientDispatcher.SendRequest(b); err != nil {
        return fmt.Errorf("%w: post-reservation delivery uncertain: %v", errQuarantined, err)
    }
    return nil
}
func isolated() bool {
    marker, err := os.ReadFile("/lab/.r6c3b2-lab6-private")
    if err != nil || string(marker) != "R6C3B2-LAB6-PRIVATE-NAMESPACE\n" { return false }
    interfaces, err := net.Interfaces()
    if err != nil || len(interfaces) != 1 || interfaces[0].Name != "lo" ||
       interfaces[0].Flags&net.FlagLoopback == 0 || interfaces[0].Flags&net.FlagUp == 0 { return false }
    routes, err := os.ReadFile("/proc/net/route")
    if err != nil || len(strings.Fields(string(routes))) < 11 { return false }
    // No routes except the table header. In particular, reject host routes.
    return len(strings.Split(strings.TrimSpace(string(routes)), "\n")) == 1
}
func run(args []string) error {
    f := flag.NewFlagSet("r6c3b2-uidlab", flag.ContinueOnError)
    journal := f.String("journal", "", "existing private /lab journal dir")
    attempt := f.String("attempt", "", "unique synthetic attempt")
    endpoint := f.String("target", "", "exact private loopback endpoint")
    if f.Parse(args) != nil || f.NArg() != 0 || *endpoint != labEndpoint ||
       !safeToken(*attempt, 80) || !strings.HasPrefix(filepath.Clean(*journal), "/lab/") ||
       !isolated() { return errQuarantined }
    st, err := os.Lstat(*journal)
    if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 || st.Mode()&os.ModeSymlink != 0 {
        return errQuarantined
    }
    socket := ws.NewClient()
    socket.SetRequestedSubProtocol(types.V16Subprotocol)
    delegate := ocppj.NewDefaultClientDispatcher(ocppj.NewFIFOClientQueue(0))
    d := newUIDDispatcher(*journal, delegate)
    client := ocppj.NewClient(labCP, socket, d, nil, core.Profile)
    client.SetDialect(ocpp.V16)
    ack := make(chan string, 1)
    client.SetResponseHandler(func(resp ocpp.Response, uid string) {
        if _, ok := resp.(*core.StartTransactionConfirmation); !ok { return }
        select { case ack <- uid: default: }
    })
    if err := client.Start(*endpoint); err != nil { return fmt.Errorf("%w: connection unavailable", errQuarantined) }
    defer client.Stop()
    req := core.NewStartTransactionRequest(1, "lab-rfid", 0, types.Now())
    if err := d.bind(req, newIntent(*attempt, "pending-bind")); err != nil { return err }
    if err := client.SendRequest(req); err != nil { return err }
    select {
    case uid := <-ack:
        record := filepath.Join(*journal, bindingName("uid", uid))
        raw, err := os.ReadFile(record)
        if err != nil { return errQuarantined }
        var found intent
        if json.Unmarshal(raw, &found) != nil || !validIntent(found) ||
            found.AttemptID != *attempt || found.UID != uid { return errQuarantined }
        fmt.Printf("LAB_ACK_OBSERVED %s\n", uid) // never asserts CleanVerified
        return nil
    case <-time.After(4*time.Second):
        return fmt.Errorf("%w: ACK unobserved; reservation retained", errQuarantined)
    }
}
func main() {
    if err := run(os.Args[1:]); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(2)
    }
}
