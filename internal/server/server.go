// Package server implements the TCP front end: it accepts connections, frames
// commands with the protocol package and hands them to a command Executor.
//
// Concurrency model: one goroutine per connection, bounded by Config.MaxConns.
// Shutdown is cooperative: when the context is cancelled the listener is
// closed, idle connections are woken up, in-flight commands are allowed to
// finish, and anything still running after Config.ShutdownTimeout is closed.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/alfonsosantana/cachekv/internal/command"
	"github.com/alfonsosantana/cachekv/internal/protocol"
)

// Executor runs a parsed command and writes its reply. It returns true if the
// connection should be closed afterwards. *command.Dispatcher implements it.
type Executor interface {
	Execute(args [][]byte, w command.Replier) (closeConn bool)
}

// Config holds server settings.
type Config struct {
	// MaxConns is the maximum number of simultaneous clients (default 10000).
	MaxConns int
	// IdleTimeout closes connections that send nothing for this long. Zero disables it.
	IdleTimeout time.Duration
	// WriteTimeout bounds how long a single reply flush may block. Zero disables it.
	WriteTimeout time.Duration
	// ShutdownTimeout is how long Serve waits for connections to finish after
	// the context is cancelled before closing them forcibly.
	ShutdownTimeout time.Duration
	// Limits bounds the size of incoming commands.
	Limits protocol.Limits
}

// DefaultConfig returns production-leaning defaults.
func DefaultConfig() Config {
	return Config{
		MaxConns:        10000,
		IdleTimeout:     5 * time.Minute,
		WriteTimeout:    10 * time.Second,
		ShutdownTimeout: 10 * time.Second,
	}
}

// Validate reports invalid settings.
func (c Config) Validate() error {
	switch {
	case c.MaxConns <= 0:
		return errors.New("max connections must be positive")
	case c.IdleTimeout < 0 || c.WriteTimeout < 0 || c.ShutdownTimeout < 0:
		return errors.New("timeouts must not be negative")
	}
	return nil
}

// Server is a TCP server speaking RESP.
type Server struct {
	cfg  Config
	exec Executor
	log  *slog.Logger

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// New creates a Server. A nil logger discards all log output.
func New(cfg Config, exec Executor, log *slog.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{cfg: cfg, exec: exec, log: log, conns: make(map[net.Conn]struct{})}, nil
}

// ListenAndServe listens on the TCP address addr and calls Serve.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	return s.Serve(ctx, ln)
}

// Serve accepts connections on ln until ctx is cancelled, then shuts down
// gracefully. It always closes ln. A clean, context-triggered shutdown returns nil.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	s.log.Info("listening", "addr", ln.Addr().String())

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close() // unblocks Accept
		case <-stop:
		}
	}()

	slots := make(chan struct{}, s.cfg.MaxConns)
	var backoff time.Duration

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				s.shutdown()
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				s.shutdown()
				return fmt.Errorf("accept: %w", err)
			}
			// Transient failure (for example EMFILE): back off and retry.
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			s.log.Warn("accept failed, retrying", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0

		select {
		case slots <- struct{}{}:
		default:
			s.log.Warn("rejecting connection: max clients reached", "remote", conn.RemoteAddr().String())
			_, _ = conn.Write([]byte("-ERR max number of clients reached\r\n"))
			_ = conn.Close()
			continue
		}

		s.track(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-slots }()
			defer s.untrack(conn)
			s.handle(ctx, conn)
		}()
	}
}

func (s *Server) track(c net.Conn) {
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	_ = c.Close()
}

// shutdown wakes idle connections, waits for the rest to drain, and forces
// the stragglers closed once ShutdownTimeout elapses.
func (s *Server) shutdown() {
	s.log.Info("shutting down", "timeout", s.cfg.ShutdownTimeout.String())

	// A read deadline in the past interrupts connections blocked waiting for
	// a command, while commands already being executed are unaffected.
	s.mu.Lock()
	for c := range s.conns {
		_ = c.SetReadDeadline(time.Now())
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(s.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		s.log.Info("all connections drained")
	case <-timer.C:
		s.mu.Lock()
		n := len(s.conns)
		for c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		s.log.Warn("shutdown timeout reached, closed connections forcibly", "connections", n)
		<-done
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	log := s.log.With("remote", remote)
	log.Debug("client connected")
	defer log.Debug("client disconnected")

	r := protocol.NewReader(conn, s.cfg.Limits)
	w := protocol.NewWriter(conn)

	for {
		if s.cfg.IdleTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		}
		// Checked after arming the deadline on purpose: if shutdown started
		// first, this sees the cancelled context; if it starts later, its
		// immediate deadline overrides ours. Either way we never sleep through it.
		if ctx.Err() != nil {
			return
		}

		args, err := r.ReadCommand()
		if err != nil {
			s.handleReadError(log, w, conn, err)
			return
		}
		if len(args) == 0 {
			continue
		}

		closeConn := s.exec.Execute(args, w)

		// Batch replies for pipelined commands: only flush once the client's
		// already-received input has been fully processed.
		if closeConn || r.Buffered() == 0 {
			if s.cfg.WriteTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
			}
			if err := w.Flush(); err != nil {
				log.Debug("write failed", "err", err)
				return
			}
		}
		if closeConn {
			return
		}
	}
}

func (s *Server) handleReadError(log *slog.Logger, w *protocol.Writer, conn net.Conn, err error) {
	var pe *protocol.Error
	switch {
	case errors.As(err, &pe):
		log.Warn("protocol error", "err", pe)
		w.Error("ERR Protocol error: " + pe.Msg)
		if s.cfg.WriteTimeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		}
		_ = w.Flush()
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
		// Normal disconnect.
	case errors.Is(err, os.ErrDeadlineExceeded):
		log.Debug("closing idle or draining connection")
	default:
		log.Debug("read failed", "err", err)
	}
}
