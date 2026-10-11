package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock, safe for concurrent use.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	return New(WithShards(4), WithClock(clk.Now)), clk
}

func TestSetGet(t *testing.T) {
	s, _ := newTestStore(t)

	if _, ok := s.Get("missing"); ok {
		t.Fatal("Get(missing) reported found")
	}

	s.Set("k", []byte("v1"), 0)
	if got, ok := s.Get("k"); !ok || string(got) != "v1" {
		t.Fatalf("Get(k) = %q, %v; want v1, true", got, ok)
	}

	s.Set("k", []byte("v2"), 0)
	if got, _ := s.Get("k"); string(got) != "v2" {
		t.Fatalf("overwrite: got %q, want v2", got)
	}
}

func TestSetCopiesInput(t *testing.T) {
	s, _ := newTestStore(t)
	buf := []byte("hello")
	s.Set("k", buf, 0)
	buf[0] = 'X'

	if got, _ := s.Get("k"); string(got) != "hello" {
		t.Fatalf("store aliased caller buffer: got %q", got)
	}
}

func TestTTLExpiry(t *testing.T) {
	s, clk := newTestStore(t)
	s.Set("k", []byte("v"), 10*time.Second)

	clk.Advance(9 * time.Second)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key expired too early")
	}

	clk.Advance(time.Second) // exactly at the deadline
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should be expired at its deadline")
	}
	if n := s.Len(); n != 0 {
		t.Fatalf("lazy expiration should purge the key; Len() = %d", n)
	}
}

func TestSetClearsPreviousTTL(t *testing.T) {
	s, clk := newTestStore(t)
	s.Set("k", []byte("a"), time.Second)
	s.Set("k", []byte("b"), 0)

	clk.Advance(time.Hour)
	if got, ok := s.Get("k"); !ok || string(got) != "b" {
		t.Fatalf("Get = %q, %v; want b, true", got, ok)
	}
}

func TestDeleteAndExists(t *testing.T) {
	s, clk := newTestStore(t)
	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	s.Set("gone", []byte("3"), time.Second)
	clk.Advance(2 * time.Second)

	if n := s.Exists("a", "b", "a", "nope", "gone"); n != 3 {
		t.Fatalf("Exists = %d, want 3 (duplicates count, expired/missing do not)", n)
	}
	if n := s.Delete("a", "nope", "gone"); n != 1 {
		t.Fatalf("Delete = %d, want 1 (expired keys are not counted)", n)
	}
	if n := s.Exists("a"); n != 0 {
		t.Fatalf("Exists(a) after delete = %d, want 0", n)
	}
}

func TestExpire(t *testing.T) {
	s, clk := newTestStore(t)

	if s.Expire("missing", time.Second) {
		t.Fatal("Expire on missing key returned true")
	}

	s.Set("k", []byte("v"), 0)
	if !s.Expire("k", 5*time.Second) {
		t.Fatal("Expire on existing key returned false")
	}
	clk.Advance(5 * time.Second)
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should have expired")
	}

	s.Set("k", []byte("v"), 0)
	if !s.Expire("k", 0) {
		t.Fatal("Expire with ttl<=0 should report the key existed")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("Expire with ttl<=0 should delete the key")
	}
}

func TestTTLReporting(t *testing.T) {
	s, clk := newTestStore(t)

	if _, st := s.TTL("missing"); st != TTLMissing {
		t.Fatalf("state = %v, want TTLMissing", st)
	}

	s.Set("forever", []byte("v"), 0)
	if _, st := s.TTL("forever"); st != TTLPersistent {
		t.Fatalf("state = %v, want TTLPersistent", st)
	}

	s.Set("temp", []byte("v"), 10*time.Second)
	clk.Advance(4 * time.Second)
	d, st := s.TTL("temp")
	if st != TTLExpiring || d != 6*time.Second {
		t.Fatalf("TTL = %v, %v; want 6s, TTLExpiring", d, st)
	}
}

func TestIncrBy(t *testing.T) {
	s, clk := newTestStore(t)

	tests := []struct {
		name    string
		setup   func()
		key     string
		delta   int64
		want    int64
		wantErr error
	}{
		{name: "creates missing key", key: "new", delta: 1, want: 1},
		{name: "increments", setup: func() { s.Set("n", []byte("41"), 0) }, key: "n", delta: 1, want: 42},
		{name: "decrements", setup: func() { s.Set("d", []byte("0"), 0) }, key: "d", delta: -5, want: -5},
		{name: "not an integer", setup: func() { s.Set("s", []byte("abc"), 0) }, key: "s", delta: 1, wantErr: ErrNotInteger},
		{name: "overflow up", setup: func() { s.Set("max", []byte("9223372036854775807"), 0) }, key: "max", delta: 1, wantErr: ErrOverflow},
		{name: "overflow down", setup: func() { s.Set("min", []byte("-9223372036854775808"), 0) }, key: "min", delta: -1, wantErr: ErrOverflow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			got, err := s.IncrBy(tt.key, tt.delta)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}

	t.Run("preserves TTL", func(t *testing.T) {
		s.Set("ttl", []byte("1"), 10*time.Second)
		if _, err := s.IncrBy("ttl", 1); err != nil {
			t.Fatal(err)
		}
		if d, st := s.TTL("ttl"); st != TTLExpiring || d != 10*time.Second {
			t.Fatalf("TTL = %v, %v; want 10s, TTLExpiring", d, st)
		}
	})

	t.Run("expired key restarts from zero", func(t *testing.T) {
		s.Set("exp", []byte("100"), time.Second)
		clk.Advance(2 * time.Second)
		if got, err := s.IncrBy("exp", 1); err != nil || got != 1 {
			t.Fatalf("got %d, %v; want 1, nil", got, err)
		}
		if _, st := s.TTL("exp"); st != TTLPersistent {
			t.Fatalf("state = %v, want TTLPersistent", st)
		}
	})
}

func TestTTLSaturatesInsteadOfOverflowing(t *testing.T) {
	s, _ := newTestStore(t)
	s.Set("k", []byte("v"), time.Duration(math.MaxInt64))
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key with huge TTL should still be readable")
	}
}

func TestDeleteExpiredAndFlush(t *testing.T) {
	s, clk := newTestStore(t)
	for i := range 10 {
		s.Set(fmt.Sprintf("short-%d", i), []byte("v"), time.Second)
		s.Set(fmt.Sprintf("long-%d", i), []byte("v"), time.Hour)
	}
	clk.Advance(2 * time.Second)

	if n := s.DeleteExpired(); n != 10 {
		t.Fatalf("DeleteExpired = %d, want 10", n)
	}
	if n := s.Len(); n != 10 {
		t.Fatalf("Len = %d, want 10", n)
	}

	s.Flush()
	if n := s.Len(); n != 0 {
		t.Fatalf("Len after Flush = %d, want 0", n)
	}
}

func TestWithShardsRoundsToPowerOfTwo(t *testing.T) {
	tests := map[int]int{-5: 1, 0: 1, 1: 1, 3: 4, 64: 64, 65: 128, 1 << 30: maxShards}
	for in, want := range tests {
		if got := len(New(WithShards(in)).shards); got != want {
			t.Errorf("WithShards(%d) -> %d shards, want %d", in, got, want)
		}
	}
}

func TestRunJanitorStopsOnCancel(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.RunJanitor(ctx, time.Millisecond)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunJanitor did not return after context cancellation")
	}
}

func TestRunJanitorPurgesExpiredKeys(t *testing.T) {
	s := New() // real clock
	s.Set("k", []byte("v"), 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.RunJanitor(ctx, time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for s.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("janitor did not purge the expired key in time")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestConcurrentAccess is primarily meaningful under `go test -race`.
func TestConcurrentAccess(_ *testing.T) {
	s := New(WithShards(8))
	const (
		workers = 16
		ops     = 2000
	)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ops {
				key := fmt.Sprintf("key-%d", i%50)
				switch (i + w) % 6 {
				case 0:
					s.Set(key, []byte("v"), time.Millisecond)
				case 1:
					s.Get(key)
				case 2:
					s.Delete(key)
				case 3:
					s.Expire(key, time.Second)
				case 4:
					_, _ = s.IncrBy("counter", 1)
				case 5:
					s.DeleteExpired()
				}
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentIncrIsAtomic(t *testing.T) {
	s := New()
	const (
		workers = 8
		each    = 1000
	)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				if _, err := s.IncrBy("c", 1); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, _ := s.Get("c")
	if want := fmt.Sprint(workers * each); string(got) != want {
		t.Fatalf("counter = %s, want %s (lost updates)", got, want)
	}
}

func BenchmarkSet(b *testing.B) {
	s := New()
	val := []byte("value")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set(fmt.Sprintf("key-%d", i%1024), val, 0)
			i++
		}
	})
}

func BenchmarkGet(b *testing.B) {
	s := New()
	for i := range 1024 {
		s.Set(fmt.Sprintf("key-%d", i), []byte("value"), 0)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key-%d", i%1024))
			i++
		}
	})
}
