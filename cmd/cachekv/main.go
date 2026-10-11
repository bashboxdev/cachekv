// Command cachekv runs an in-memory, Redis-compatible key-value server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alfonsosantana/cachekv/internal/command"
	"github.com/alfonsosantana/cachekv/internal/server"
	"github.com/alfonsosantana/cachekv/internal/store"
)

// Set at build time: -ldflags "-X main.version=v1.2.3".
var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "cachekv:", err)
		os.Exit(1)
	}
}

type options struct {
	addr            string
	shards          int
	janitorInterval time.Duration
	logLevel        string
	logJSON         bool
	showVersion     bool
	server          server.Config
}

func parseFlags(args []string, out io.Writer) (options, error) {
	def := server.DefaultConfig()
	o := options{server: def}

	fs := flag.NewFlagSet("cachekv", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&o.addr, "addr", ":6379", "TCP address to listen on")
	fs.IntVar(&o.shards, "shards", 64, "number of store shards (rounded up to a power of two)")
	fs.DurationVar(&o.janitorInterval, "janitor-interval", time.Second, "how often expired keys are swept")
	fs.IntVar(&o.server.MaxConns, "max-conns", def.MaxConns, "maximum simultaneous client connections")
	fs.DurationVar(&o.server.IdleTimeout, "idle-timeout", def.IdleTimeout, "close clients idle for this long (0 disables)")
	fs.DurationVar(&o.server.WriteTimeout, "write-timeout", def.WriteTimeout, "max time to flush a reply (0 disables)")
	fs.DurationVar(&o.server.ShutdownTimeout, "shutdown-timeout", def.ShutdownTimeout, "grace period for in-flight connections on shutdown")
	fs.IntVar(&o.server.Limits.MaxBulkLen, "max-bulk-len", 0, "max size in bytes of one command argument (0 = default 1 MiB)")
	fs.StringVar(&o.logLevel, "log-level", "info", "log level: debug, info, warn or error")
	fs.BoolVar(&o.logJSON, "log-json", false, "emit logs as JSON")
	fs.BoolVar(&o.showVersion, "version", false, "print version and exit")

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	return o, nil
}

func newLogger(w io.Writer, level string, asJSON bool) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid -log-level %q", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if asJSON {
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	}
	return slog.New(slog.NewTextHandler(w, opts)), nil
}

func run(args []string, stderr io.Writer) error {
	opts, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if opts.showVersion {
		fmt.Println("cachekv", version)
		return nil
	}

	log, err := newLogger(stderr, opts.logLevel, opts.logJSON)
	if err != nil {
		return err
	}

	// Cancelled on SIGINT/SIGTERM, or explicitly if the server fails to start.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := store.New(store.WithShards(opts.shards))
	srv, err := server.New(opts.server, command.NewDispatcher(st), log)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		st.RunJanitor(ctx, opts.janitorInterval)
	}()

	log.Info("starting cachekv", "version", version, "addr", opts.addr, "shards", opts.shards)
	err = srv.ListenAndServe(ctx, opts.addr)

	stop()    // make sure background goroutines see the cancellation
	wg.Wait() // and wait for them before exiting
	return err
}
