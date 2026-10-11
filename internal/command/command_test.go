package command

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfonsosantana/cachekv/internal/protocol"
	"github.com/alfonsosantana/cachekv/internal/store"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// run executes one command line (space-separated) and returns the raw RESP reply.
func run(t *testing.T, d *Dispatcher, line string) (reply string, closed bool) {
	t.Helper()
	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)
	fields := strings.Fields(line)
	args := make([][]byte, 0, len(fields))
	for _, f := range fields {
		args = append(args, []byte(f))
	}
	closed = d.Execute(args, w)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf.String(), closed
}

func TestCommands(t *testing.T) {
	clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	d := NewDispatcher(store.New(store.WithShards(2), store.WithClock(clk.now)))

	steps := []struct {
		cmd     string
		want    string
		advance time.Duration // applied after the command
	}{
		// connection / misc
		{cmd: "PING", want: "+PONG\r\n"},
		{cmd: "ping hello", want: "$5\r\nhello\r\n"},
		{cmd: "ECHO hi", want: "$2\r\nhi\r\n"},
		{cmd: "COMMAND DOCS", want: "*0\r\n"},

		// SET / GET
		{cmd: "GET k", want: "$-1\r\n"},
		{cmd: "SET k v", want: "+OK\r\n"},
		{cmd: "get k", want: "$1\r\nv\r\n"},
		{cmd: "SET k", want: "-ERR wrong number of arguments for 'set' command\r\n"},
		{cmd: "SET k v EX", want: "-ERR syntax error\r\n"},
		{cmd: "SET k v NX 1", want: "-ERR syntax error\r\n"},
		{cmd: "SET k v EX abc", want: "-ERR value is not an integer or out of range\r\n"},
		{cmd: "SET k v EX 0", want: "-ERR invalid expire time in 'set' command\r\n"},
		{cmd: "SET k v EX -3", want: "-ERR invalid expire time in 'set' command\r\n"},
		{cmd: "SET k v EX 9999999999999", want: "-ERR invalid expire time in 'set' command\r\n"},

		// TTL handling
		{cmd: "TTL k", want: ":-1\r\n"},
		{cmd: "TTL nope", want: ":-2\r\n"},
		{cmd: "SET t v EX 10", want: "+OK\r\n", advance: 4 * time.Second},
		{cmd: "TTL t", want: ":6\r\n"},
		{cmd: "PTTL t", want: ":6000\r\n", advance: 6 * time.Second},
		{cmd: "GET t", want: "$-1\r\n"},
		{cmd: "SET p v PX 1500", want: "+OK\r\n"},
		{cmd: "PTTL p", want: ":1500\r\n"},
		{cmd: "EXPIRE k 100", want: ":1\r\n"},
		{cmd: "TTL k", want: ":100\r\n"},
		{cmd: "EXPIRE nope 100", want: ":0\r\n"},
		{cmd: "EXPIRE k abc", want: "-ERR value is not an integer or out of range\r\n"},
		{cmd: "EXPIRE k 9999999999999", want: "-ERR invalid expire time in 'expire' command\r\n"},
		{cmd: "EXPIRE k -1", want: ":1\r\n"},
		{cmd: "EXISTS k", want: ":0\r\n"},

		// DEL / EXISTS / DBSIZE / FLUSHALL
		{cmd: "FLUSHALL", want: "+OK\r\n"},
		{cmd: "SET a 1", want: "+OK\r\n"},
		{cmd: "SET b 2", want: "+OK\r\n"},
		{cmd: "EXISTS a b a c", want: ":3\r\n"},
		{cmd: "DBSIZE", want: ":2\r\n"},
		{cmd: "DEL a c", want: ":1\r\n"},
		{cmd: "DEL", want: "-ERR wrong number of arguments for 'del' command\r\n"},
		{cmd: "FLUSHALL BOGUS", want: "-ERR syntax error\r\n"},
		{cmd: "FLUSHALL ASYNC", want: "+OK\r\n"},
		{cmd: "DBSIZE", want: ":0\r\n"},

		// counters
		{cmd: "INCR n", want: ":1\r\n"},
		{cmd: "INCR n", want: ":2\r\n"},
		{cmd: "DECR n", want: ":1\r\n"},
		{cmd: "INCRBY n 41", want: ":42\r\n"},
		{cmd: "INCRBY n abc", want: "-ERR value is not an integer or out of range\r\n"},
		{cmd: "SET s text", want: "+OK\r\n"},
		{cmd: "INCR s", want: "-ERR value is not an integer or out of range\r\n"},
		{cmd: "SET big 9223372036854775807", want: "+OK\r\n"},
		{cmd: "INCR big", want: "-ERR increment or decrement would overflow\r\n"},

		// errors
		{cmd: "NOSUCH x", want: "-ERR unknown command 'NOSUCH'\r\n"},
		{cmd: "GET", want: "-ERR wrong number of arguments for 'get' command\r\n"},
		{cmd: "GET a b", want: "-ERR wrong number of arguments for 'get' command\r\n"},
	}

	for _, s := range steps {
		got, closed := run(t, d, s.cmd)
		if got != s.want {
			t.Errorf("%-28s => %q, want %q", s.cmd, got, s.want)
		}
		if closed {
			t.Errorf("%s unexpectedly closed the connection", s.cmd)
		}
		clk.advance(s.advance)
	}
}

func TestQuitClosesConnection(t *testing.T) {
	d := NewDispatcher(store.New())
	reply, closed := run(t, d, "QUIT")
	if reply != "+OK\r\n" || !closed {
		t.Fatalf("QUIT => %q, closed=%v; want +OK, true", reply, closed)
	}
}

func TestExecuteIgnoresEmptyCommand(t *testing.T) {
	d := NewDispatcher(store.New())
	var buf bytes.Buffer
	if d.Execute(nil, protocol.NewWriter(&buf)) || buf.Len() != 0 {
		t.Fatal("empty command must produce no reply")
	}
}

func TestUnknownCommandNameIsTruncated(t *testing.T) {
	d := NewDispatcher(store.New())
	reply, _ := run(t, d, strings.Repeat("x", 500))
	if len(reply) > 120 {
		t.Fatalf("unknown-command reply echoes too much user input (%d bytes)", len(reply))
	}
}

// panickingStore lets us verify that a faulty handler is contained.
type panickingStore struct{ Store }

func (panickingStore) Get(string) ([]byte, bool) { panic("boom") }

func TestPanicInHandlerBecomesErrorReply(t *testing.T) {
	d := NewDispatcher(panickingStore{store.New()})
	reply, closed := run(t, d, "GET k")
	if !strings.HasPrefix(reply, "-ERR internal error") || closed {
		t.Fatalf("got %q, closed=%v", reply, closed)
	}
	// The dispatcher must remain usable afterwards.
	if reply, _ := run(t, d, "PING"); reply != "+PONG\r\n" {
		t.Fatalf("dispatcher broken after panic: %q", reply)
	}
}
