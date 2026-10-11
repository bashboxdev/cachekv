package store

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"
)

// Errors returned by numeric operations.
var (
	// ErrNotInteger is returned when a value cannot be parsed as a 64-bit integer.
	ErrNotInteger = errors.New("value is not an integer or out of range")
	// ErrOverflow is returned when an increment would overflow int64.
	ErrOverflow = errors.New("increment or decrement would overflow")
)

// TTLState describes the expiration status of a key.
type TTLState int

const (
	// TTLMissing means the key does not exist (or has already expired).
	TTLMissing TTLState = iota
	// TTLPersistent means the key exists and has no expiration.
	TTLPersistent
	// TTLExpiring means the key exists and will expire; see the returned duration.
	TTLExpiring
)

const (
	defaultShards = 64
	maxShards     = 1 << 16
)

type entry struct {
	value     []byte
	expiresAt int64 // unix nanoseconds; 0 means "never expires"
}

func (e entry) expired(now int64) bool {
	return e.expiresAt != 0 && now >= e.expiresAt
}

type shard struct {
	mu    sync.RWMutex
	items map[string]entry
}

// Store is a concurrent in-memory key-value store. The zero value is not
// usable; create one with New.
type Store struct {
	shards []shard
	mask   uint32
	now    func() time.Time
}

// Option configures a Store.
type Option func(*Store)

// WithShards sets the number of shards. The value is rounded up to the next
// power of two and clamped to [1, 65536].
func WithShards(n int) Option {
	return func(s *Store) {
		n = max(1, min(n, maxShards))
		size := 1
		for size < n {
			size <<= 1
		}
		s.shards = make([]shard, size)
	}
}

// WithClock overrides the time source. It is intended for tests.
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// New creates an empty Store.
func New(opts ...Option) *Store {
	s := &Store{now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	if s.shards == nil {
		s.shards = make([]shard, defaultShards)
	}
	s.mask = uint32(len(s.shards) - 1) //nolint:gosec // len is bounded by maxShards
	for i := range s.shards {
		s.shards[i].items = make(map[string]entry)
	}
	return s
}

// fnv32a is an allocation-free FNV-1a hash over a string.
func fnv32a(s string) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	for i := range len(s) {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

func (s *Store) shardFor(key string) *shard {
	return &s.shards[fnv32a(key)&s.mask]
}

func (s *Store) nowNano() int64 { return s.now().UnixNano() }

// expiryFor converts a TTL to an absolute deadline. ttl <= 0 means no expiry.
func expiryFor(now int64, ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	if int64(ttl) > math.MaxInt64-now {
		return math.MaxInt64 // saturate instead of overflowing
	}
	return now + int64(ttl)
}

// Set stores a copy of value under key. A ttl <= 0 means the key never expires.
// Any previous value and TTL are replaced.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	v := make([]byte, len(value))
	copy(v, value)

	sh := s.shardFor(key)
	now := s.nowNano()
	sh.mu.Lock()
	sh.items[key] = entry{value: v, expiresAt: expiryFor(now, ttl)}
	sh.mu.Unlock()
}

// Get returns the value stored under key. The returned slice must not be modified.
func (s *Store) Get(key string) ([]byte, bool) {
	sh := s.shardFor(key)
	now := s.nowNano()

	sh.mu.RLock()
	e, ok := sh.items[key]
	sh.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if e.expired(now) {
		s.deleteIfExpired(sh, key, now)
		return nil, false
	}
	return e.value, true
}

// deleteIfExpired removes key only if it is still expired, guarding against a
// concurrent Set between the read-unlock and write-lock.
func (s *Store) deleteIfExpired(sh *shard, key string, now int64) {
	sh.mu.Lock()
	if e, ok := sh.items[key]; ok && e.expired(now) {
		delete(sh.items, key)
	}
	sh.mu.Unlock()
}

// Delete removes the given keys and returns how many existing (non-expired)
// keys were removed.
func (s *Store) Delete(keys ...string) int {
	now := s.nowNano()
	removed := 0
	for _, key := range keys {
		sh := s.shardFor(key)
		sh.mu.Lock()
		if e, ok := sh.items[key]; ok {
			if !e.expired(now) {
				removed++
			}
			delete(sh.items, key)
		}
		sh.mu.Unlock()
	}
	return removed
}

// Exists returns how many of the given keys exist. A key listed more than once
// is counted more than once, matching Redis semantics.
func (s *Store) Exists(keys ...string) int {
	now := s.nowNano()
	count := 0
	for _, key := range keys {
		sh := s.shardFor(key)
		sh.mu.RLock()
		e, ok := sh.items[key]
		sh.mu.RUnlock()
		if ok && !e.expired(now) {
			count++
		}
	}
	return count
}

// Expire sets a TTL on an existing key and reports whether the key existed.
// A ttl <= 0 deletes the key immediately.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	sh := s.shardFor(key)
	now := s.nowNano()

	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.items[key]
	if !ok || e.expired(now) {
		delete(sh.items, key)
		return false
	}
	if ttl <= 0 {
		delete(sh.items, key)
		return true
	}
	e.expiresAt = expiryFor(now, ttl)
	sh.items[key] = e
	return true
}

// TTL reports the remaining time to live of key.
func (s *Store) TTL(key string) (time.Duration, TTLState) {
	sh := s.shardFor(key)
	now := s.nowNano()

	sh.mu.RLock()
	e, ok := sh.items[key]
	sh.mu.RUnlock()
	switch {
	case !ok || e.expired(now):
		return 0, TTLMissing
	case e.expiresAt == 0:
		return 0, TTLPersistent
	default:
		return time.Duration(e.expiresAt - now), TTLExpiring
	}
}

// IncrBy atomically adds delta to the integer stored at key, creating the key
// with value delta if it does not exist. An existing TTL is preserved.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	sh := s.shardFor(key)
	now := s.nowNano()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.items[key]
	var cur int64
	if ok && !e.expired(now) {
		n, err := strconv.ParseInt(string(e.value), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		cur = n
	} else {
		e = entry{} // missing or expired: start from zero with no TTL
	}

	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	cur += delta
	e.value = strconv.AppendInt(nil, cur, 10)
	sh.items[key] = e
	return cur, nil
}

// Len returns the number of stored keys. It may include keys that have expired
// but have not been purged yet.
func (s *Store) Len() int {
	total := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		total += len(sh.items)
		sh.mu.RUnlock()
	}
	return total
}

// Flush removes every key.
func (s *Store) Flush() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sh.items = make(map[string]entry)
		sh.mu.Unlock()
	}
}

// DeleteExpired removes all expired keys and returns how many were removed.
// Each shard is locked independently, so a sweep never blocks the whole store.
func (s *Store) DeleteExpired() int {
	now := s.nowNano()
	removed := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, e := range sh.items {
			if e.expired(now) {
				delete(sh.items, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// RunJanitor sweeps expired keys every interval until ctx is cancelled. It
// blocks, so callers normally run it in its own goroutine and wait for it to
// return during shutdown. A non-positive interval returns immediately.
func (s *Store) RunJanitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.DeleteExpired()
		}
	}
}
