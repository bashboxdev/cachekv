package command

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/alfonsosantana/cachekv/internal/store"
)

const (
	errSyntax     = "ERR syntax error"
	errNotInteger = "ERR value is not an integer or out of range"

	// maxTTLSeconds keeps seconds*time.Second within the range of time.Duration.
	maxTTLSeconds = math.MaxInt64 / int64(time.Second)
	maxTTLMillis  = math.MaxInt64 / int64(time.Millisecond)
)

func builtins() []spec {
	return []spec{
		{name: "PING", minArgs: 0, maxArgs: 1, run: ping},
		{name: "ECHO", minArgs: 1, maxArgs: 1, run: echo},
		{name: "SET", minArgs: 2, maxArgs: 4, run: set},
		{name: "GET", minArgs: 1, maxArgs: 1, run: get},
		{name: "DEL", minArgs: 1, maxArgs: unlimited, run: del},
		{name: "EXISTS", minArgs: 1, maxArgs: unlimited, run: exists},
		{name: "EXPIRE", minArgs: 2, maxArgs: 2, run: expire},
		{name: "TTL", minArgs: 1, maxArgs: 1, run: ttl(time.Second)},
		{name: "PTTL", minArgs: 1, maxArgs: 1, run: ttl(time.Millisecond)},
		{name: "INCR", minArgs: 1, maxArgs: 1, run: incrFixed(1)},
		{name: "DECR", minArgs: 1, maxArgs: 1, run: incrFixed(-1)},
		{name: "INCRBY", minArgs: 2, maxArgs: 2, run: incrBy},
		{name: "DBSIZE", minArgs: 0, maxArgs: 0, run: dbsize},
		{name: "FLUSHALL", minArgs: 0, maxArgs: 1, run: flushall},
		// redis-cli sends COMMAND DOCS on connect; an empty reply keeps it happy.
		{name: "COMMAND", minArgs: 0, maxArgs: unlimited, run: func(_ Store, _ [][]byte, w Replier) { w.Array(0) }},
		{name: "QUIT", minArgs: 0, maxArgs: 0, closeAfter: true, run: func(_ Store, _ [][]byte, w Replier) { w.Simple("OK") }},
	}
}

func ping(_ Store, args [][]byte, w Replier) {
	if len(args) == 1 {
		w.Bulk(args[0])
		return
	}
	w.Simple("PONG")
}

func echo(_ Store, args [][]byte, w Replier) { w.Bulk(args[0]) }

// set implements SET key value [EX seconds | PX milliseconds].
func set(st Store, args [][]byte, w Replier) {
	var ttl time.Duration
	switch len(args) {
	case 2:
	case 4:
		var unit time.Duration
		var limit int64
		switch strings.ToUpper(string(args[2])) {
		case "EX":
			unit, limit = time.Second, maxTTLSeconds
		case "PX":
			unit, limit = time.Millisecond, maxTTLMillis
		default:
			w.Error(errSyntax)
			return
		}
		n, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			w.Error(errNotInteger)
			return
		}
		if n <= 0 || n > limit {
			w.Error("ERR invalid expire time in 'set' command")
			return
		}
		ttl = time.Duration(n) * unit
	default: // 3 arguments: an option without a value
		w.Error(errSyntax)
		return
	}
	st.Set(string(args[0]), args[1], ttl)
	w.Simple("OK")
}

func get(st Store, args [][]byte, w Replier) {
	if v, ok := st.Get(string(args[0])); ok {
		w.Bulk(v)
		return
	}
	w.Null()
}

func keys(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

func del(st Store, args [][]byte, w Replier) { w.Integer(int64(st.Delete(keys(args)...))) }

func exists(st Store, args [][]byte, w Replier) { w.Integer(int64(st.Exists(keys(args)...))) }

func expire(st Store, args [][]byte, w Replier) {
	secs, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		w.Error(errNotInteger)
		return
	}
	if secs > maxTTLSeconds {
		w.Error("ERR invalid expire time in 'expire' command")
		return
	}
	// A negative value is allowed: the store deletes the key immediately.
	if secs < 0 {
		secs = 0
	}
	if st.Expire(string(args[0]), time.Duration(secs)*time.Second) {
		w.Integer(1)
		return
	}
	w.Integer(0)
}

// ttl builds TTL/PTTL. Replies follow Redis: -2 missing, -1 no expiry.
func ttl(unit time.Duration) handler {
	return func(st Store, args [][]byte, w Replier) {
		d, state := st.TTL(string(args[0]))
		switch state {
		case store.TTLMissing:
			w.Integer(-2)
		case store.TTLPersistent:
			w.Integer(-1)
		case store.TTLExpiring:
			// Round to the nearest unit, like Redis does.
			w.Integer(int64((d + unit/2) / unit))
		}
	}
}

func replyIncr(w Replier, n int64, err error) {
	switch {
	case errors.Is(err, store.ErrNotInteger):
		w.Error(errNotInteger)
	case errors.Is(err, store.ErrOverflow):
		w.Error("ERR increment or decrement would overflow")
	case err != nil:
		w.Error("ERR " + err.Error())
	default:
		w.Integer(n)
	}
}

func incrFixed(delta int64) handler {
	return func(st Store, args [][]byte, w Replier) {
		n, err := st.IncrBy(string(args[0]), delta)
		replyIncr(w, n, err)
	}
}

func incrBy(st Store, args [][]byte, w Replier) {
	delta, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		w.Error(errNotInteger)
		return
	}
	n, err := st.IncrBy(string(args[0]), delta)
	replyIncr(w, n, err)
}

func dbsize(st Store, _ [][]byte, w Replier) { w.Integer(int64(st.Len())) }

func flushall(st Store, args [][]byte, w Replier) {
	if len(args) == 1 {
		if opt := strings.ToUpper(string(args[0])); opt != "SYNC" && opt != "ASYNC" {
			w.Error(errSyntax)
			return
		}
	}
	st.Flush()
	w.Simple("OK")
}
