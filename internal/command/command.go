// Package command maps parsed client commands onto store operations and
// encodes the replies. It depends only on two small interfaces (Store and
// Replier), so it can be tested without sockets and without a real store.
package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/alfonsosantana/cachekv/internal/store"
)

// Store is the subset of store.Store used by the commands.
type Store interface {
	Set(key string, value []byte, ttl time.Duration)
	Get(key string) ([]byte, bool)
	Delete(keys ...string) int
	Exists(keys ...string) int
	Expire(key string, ttl time.Duration) bool
	TTL(key string) (time.Duration, store.TTLState)
	IncrBy(key string, delta int64) (int64, error)
	Len() int
	Flush()
}

// Replier encodes replies. *protocol.Writer implements it.
type Replier interface {
	Simple(s string)
	Error(msg string)
	Integer(n int64)
	Bulk(b []byte)
	Null()
	Array(n int)
}

// unlimited marks a command that accepts any number of extra arguments.
const unlimited = -1

type handler func(st Store, args [][]byte, w Replier)

// spec describes a command. minArgs and maxArgs count arguments after the
// command name.
type spec struct {
	name       string
	minArgs    int
	maxArgs    int
	closeAfter bool // the server should close the connection after replying
	run        handler
}

// Dispatcher executes commands against a Store. It is safe for concurrent use
// because it holds no mutable state.
type Dispatcher struct {
	store    Store
	commands map[string]spec
}

// NewDispatcher returns a Dispatcher with the built-in commands registered.
func NewDispatcher(st Store) *Dispatcher {
	d := &Dispatcher{store: st, commands: make(map[string]spec)}
	for _, c := range builtins() {
		d.commands[c.name] = c
	}
	return d
}

// Execute runs one command, where args[0] is the command name, and writes the
// reply to w. It returns true if the connection should be closed afterwards.
// A panicking handler is converted into an error reply so one bad command
// cannot take the server down.
func (d *Dispatcher) Execute(args [][]byte, w Replier) (closeConn bool) {
	if len(args) == 0 {
		return false
	}
	name := strings.ToUpper(string(args[0]))

	defer func() {
		if r := recover(); r != nil {
			w.Error(fmt.Sprintf("ERR internal error while executing '%s'", strings.ToLower(name)))
			closeConn = false
		}
	}()

	c, ok := d.commands[name]
	if !ok {
		w.Error(fmt.Sprintf("ERR unknown command '%s'", truncate(string(args[0]), 64)))
		return false
	}
	if n := len(args) - 1; n < c.minArgs || (c.maxArgs != unlimited && n > c.maxArgs) {
		w.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(c.name)))
		return false
	}
	c.run(d.store, args[1:], w)
	return c.closeAfter
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
