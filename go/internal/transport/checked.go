// Package transport provides a strict WebSocket adapter for ocpp-go.
//
// The upstream ws.Client offers subprotocols but doesn't check that the
// server selected one. This adapter closes that gap without reimplementing
// any OCPP messages, routing, or validation.
package transport

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lorenzodonini/ocpp-go/ws"
)

// CheckedClient implements ws.WsClient and requires an agreed subprotocol.
type CheckedClient struct {
	mu          sync.RWMutex
	writeMu     sync.Mutex
	conn        *websocket.Conn
	config      ws.ClientTimeoutConfig
	tlsConfig   *tls.Config
	protocol    string
	headers     http.Header
	options     []func(*websocket.Dialer)
	onMessage   func([]byte) error
	onDisconnect func(error)
	onReconnect func()
	connected   bool
	stopped     bool
	errors      chan error
	disconnected chan error
}

func NewCheckedClient(tlsConfig *tls.Config, timeout time.Duration) *CheckedClient {
	config := ws.NewClientTimeoutConfig()
	if timeout > 0 {
		config.HandshakeTimeout = timeout
	}
	return &CheckedClient{
		config: config, tlsConfig: tlsConfig,
		headers: make(http.Header), errors: make(chan error, 8),
		disconnected: make(chan error, 1),
	}
}

func (c *CheckedClient) Disconnected() <-chan error { return c.disconnected }

func (c *CheckedClient) Start(address string) error {
	c.mu.Lock()
	if c.connected {
		c.mu.Unlock()
		return fmt.Errorf("client already connected")
	}
	cfg := c.config
	tlsCfg := c.tlsConfig
	proto := c.protocol
	headers := c.headers.Clone()
	opts := append([]func(*websocket.Dialer){}, c.options...)
	c.mu.Unlock()

	d := websocket.Dialer{HandshakeTimeout: cfg.HandshakeTimeout, TLSClientConfig: tlsCfg}
	for _, option := range opts {
		option(&d)
	}
	if proto == "" {
		return fmt.Errorf("missing WebSocket subprotocol")
	}
	if len(d.Subprotocols) == 0 {
		d.Subprotocols = []string{proto}
	}
	conn, response, err := d.Dial(address, headers)
	if err != nil {
		if response != nil {
			return fmt.Errorf("websocket handshake (HTTP %d): %w", response.StatusCode, err)
		}
		return fmt.Errorf("websocket handshake: %w", err)
	}
	if got := conn.Subprotocol(); got != proto {
		_ = conn.Close()
		return fmt.Errorf("server selected WebSocket subprotocol %q, expected %q", got, proto)
	}
	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.stopped = false
	c.mu.Unlock()
	go c.readLoop(conn)
	return nil
}

// The upstream library uses Start, not StartWithRetries. G1 intentionally
// provides no retry policy; this method reports a single failed attempt.
func (c *CheckedClient) StartWithRetries(address string) {
	if err := c.Start(address); err != nil {
		c.report(err)
	}
}

func (c *CheckedClient) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			c.disconnect(conn, err)
			return
		}
		c.mu.RLock()
		handler := c.onMessage
		c.mu.RUnlock()
		if handler != nil {
			if err := handler(data); err != nil {
				c.report(fmt.Errorf("OCPP message handler: %w", err))
				c.disconnect(conn, err)
				return
			}
		}
	}
}

func (c *CheckedClient) disconnect(conn *websocket.Conn, reason error) {
	c.mu.Lock()
	if c.conn != conn || !c.connected {
		c.mu.Unlock()
		return
	}
	c.connected = false
	manual := c.stopped
	callback := c.onDisconnect
	c.mu.Unlock()
	_ = conn.Close()
	if !manual {
		select {
		case c.disconnected <- reason:
		default:
		}
		if callback != nil {
			callback(reason)
		}
	}
}

func (c *CheckedClient) Stop() {
	c.mu.Lock()
	conn := c.conn
	connected := c.connected
	c.stopped = true
	c.connected = false
	callback := c.onDisconnect
	c.mu.Unlock()
	if conn != nil {
		c.writeMu.Lock()
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = conn.Close()
		c.writeMu.Unlock()
	}
	// ocppj.Client.Stop temporarily installs this callback and waits for it,
	// so invoke it even on deliberate shutdown.
	if connected && callback != nil {
		callback(nil)
	}
}

func (c *CheckedClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *CheckedClient) Write(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.RLock()
	conn, connected := c.conn, c.connected
	wait := c.config.WriteWait
	c.mu.RUnlock()
	if !connected || conn == nil {
		return fmt.Errorf("websocket disconnected")
	}
	if wait > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(wait))
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

func (c *CheckedClient) Errors() <-chan error { return c.errors }
func (c *CheckedClient) report(err error) {
	select {
	case c.errors <- err:
	default:
	}
}
func (c *CheckedClient) SetMessageHandler(fn func([]byte) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onMessage = fn
}
func (c *CheckedClient) SetTimeoutConfig(cfg ws.ClientTimeoutConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = cfg
}
func (c *CheckedClient) SetDisconnectedHandler(fn func(error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onDisconnect = fn
}
func (c *CheckedClient) SetReconnectedHandler(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onReconnect = fn
}
func (c *CheckedClient) AddOption(option interface{}) {
	if fn, ok := option.(func(*websocket.Dialer)); ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.options = append(c.options, fn)
	}
}
func (c *CheckedClient) SetRequestedSubProtocol(sub string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.protocol = sub
}
func (c *CheckedClient) SetBasicAuth(user, password string) {
	c.SetHeaderValue("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":"+password)))
}
func (c *CheckedClient) SetHeaderValue(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers.Set(key, value)
}
