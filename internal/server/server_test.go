package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfonsosantana/cachekv/internal/command"
	"github.com/alfonsosantana/cachekv/internal/store"
)

const testTimeout = 5 * time.Second

type harness struct {
	addr    string
	cancel  context.CancelFunc
	stopped chan struct{} // closed when Serve has returned
	err     error         // Serve's result; valid once stopped is closed
}

// wait blocks until Serve returns and reports its result.
func (h *harness) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-h.stopped:
		return h.err
	case <-time.After(testTimeout):
		t.Fatal("Serve did not return in time")
		return nil
	}
}

// start runs a server on a random local port and stops it when the test ends.
func start(t *testing.T, cfg Config, exec Executor) *harness {
	t.Helper()
	if exec == nil {
		exec = command.NewDispatcher(store.New())
	}
	srv, err := New(cfg, exec, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{addr: ln.Addr().String(), cancel: cancel, stopped: make(chan struct{})}
	go func() {
		h.err = srv.Serve(ctx, ln)
		close(h.stopped)
	}()

	t.Cleanup(func() {
		cancel()
		h.wait(t)
	})
	return h
}

type client struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func (h *harness) dial(t *testing.T) *client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", h.addr, testTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &client{t: t, conn: conn, br: bufio.NewReader(conn)}
}

func (c *client) send(s string) {
	c.t.Helper()
	_ = c.conn.SetWriteDeadline(time.Now().Add(testTimeout))
	if _, err := io.WriteString(c.conn, s); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

func (c *client) expect(want string) {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
	got := make([]byte, len(want))
	if _, err := io.ReadFull(c.br, got); err != nil {
		c.t.Fatalf("read (want %q, got %q): %v", want, got, err)
	}
	if string(got) != want {
		c.t.Fatalf("got %q, want %q", got, want)
	}
}

// expectClosed waits for the server to close the connection.
func (c *client) expectClosed() {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
	if _, err := c.br.ReadByte(); err != io.EOF {
		c.t.Fatalf("expected EOF (connection closed by server), got %v", err)
	}
}

func resp(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.String()
}

func TestRESPCommands(t *testing.T) {
	c := start(t, DefaultConfig(), nil).dial(t)

	c.send(resp("PING"))
	c.expect("+PONG\r\n")
	c.send(resp("SET", "greeting", "hello world"))
	c.expect("+OK\r\n")
	c.send(resp("GET", "greeting"))
	c.expect("$11\r\nhello world\r\n")
	c.send(resp("DEL", "greeting"))
	c.expect(":1\r\n")
	c.send(resp("GET", "greeting"))
	c.expect("$-1\r\n")
}

func TestInlineCommands(t *testing.T) {
	c := start(t, DefaultConfig(), nil).dial(t)
	c.send("SET k v\r\n")
	c.expect("+OK\r\n")
	c.send("GET k\r\n")
	c.expect("$1\r\nv\r\n")
}

func TestPipelining(t *testing.T) {
	c := start(t, DefaultConfig(), nil).dial(t)
	c.send(resp("SET", "a", "1") + resp("INCR", "a") + resp("GET", "a") + resp("PING"))
	c.expect("+OK\r\n:2\r\n$1\r\n2\r\n+PONG\r\n")
}

func TestQuitClosesConnection(t *testing.T) {
	c := start(t, DefaultConfig(), nil).dial(t)
	c.send(resp("QUIT"))
	c.expect("+OK\r\n")
	c.expectClosed()
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	c := start(t, DefaultConfig(), nil).dial(t)
	c.send("*1\r\n:oops\r\n")
	c.expect("-ERR Protocol error: expected '$', got ':'\r\n")
	c.expectClosed()
}

func TestOversizedBulkIsRejected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Limits.MaxBulkLen = 8
	c := start(t, cfg, nil).dial(t)
	c.send("*2\r\n$3\r\nGET\r\n$9999\r\n")
	c.expect("-ERR Protocol error: invalid bulk length\r\n")
	c.expectClosed()
}

func TestMaxConnsRejectsExtraClients(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConns = 1
	h := start(t, cfg, nil)

	first := h.dial(t)
	first.send(resp("PING"))
	first.expect("+PONG\r\n") // the slot is definitely taken now

	second := h.dial(t)
	second.expect("-ERR max number of clients reached\r\n")
	second.expectClosed()

	// The first client is unaffected.
	first.send(resp("PING"))
	first.expect("+PONG\r\n")
}

func TestIdleTimeoutClosesConnection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IdleTimeout = 50 * time.Millisecond
	c := start(t, cfg, nil).dial(t)
	c.expectClosed()
}

func TestConcurrentClients(t *testing.T) {
	h := start(t, DefaultConfig(), nil)
	const (
		clients = 32
		incrs   = 100
	)

	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", h.addr, testTimeout)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			br := bufio.NewReader(conn)
			_ = conn.SetDeadline(time.Now().Add(testTimeout))
			for range incrs {
				if _, err := io.WriteString(conn, resp("INCR", "counter")); err != nil {
					t.Error(err)
					return
				}
				if _, err := br.ReadString('\n'); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	c := h.dial(t)
	c.send(resp("GET", "counter"))
	want := fmt.Sprint(clients * incrs)
	c.expect(fmt.Sprintf("$%d\r\n%s\r\n", len(want), want))
}

func TestGracefulShutdownClosesIdleConnections(t *testing.T) {
	h := start(t, DefaultConfig(), nil)
	c := h.dial(t)
	c.send(resp("PING"))
	c.expect("+PONG\r\n")

	start := time.Now()
	h.cancel()

	if err := h.wait(t); err != nil {
		t.Fatalf("Serve returned %v, want nil on graceful shutdown", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("shutdown of an idle connection took %v; it should be near-instant", d)
	}
	c.expectClosed()
}

// blockingExec simulates a command that hangs, to exercise the forced-close path.
type blockingExec struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingExec) Execute(_ [][]byte, w command.Replier) bool {
	b.entered <- struct{}{}
	<-b.release
	w.Simple("OK")
	return false
}

func TestShutdownTimeoutForcesConnectionsClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShutdownTimeout = 100 * time.Millisecond
	exec := &blockingExec{entered: make(chan struct{}, 1), release: make(chan struct{})}
	h := start(t, cfg, exec)

	c := h.dial(t)
	c.send(resp("SLOW"))
	<-exec.entered // the handler is now stuck inside Execute

	h.cancel()
	c.expectClosed() // closed forcibly once ShutdownTimeout elapsed

	close(exec.release) // let the stuck handler finish so Serve can return
	if err := h.wait(t); err != nil {
		t.Fatalf("Serve returned %v, want nil", err)
	}
}

func TestConfigValidate(t *testing.T) {
	bad := []Config{
		{MaxConns: 0},
		{MaxConns: 1, IdleTimeout: -1},
		{MaxConns: 1, WriteTimeout: -1},
		{MaxConns: 1, ShutdownTimeout: -1},
	}
	for i, cfg := range bad {
		if _, err := New(cfg, nil, nil); err == nil {
			t.Errorf("case %d: expected a validation error", i)
		}
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config must be valid: %v", err)
	}
}
