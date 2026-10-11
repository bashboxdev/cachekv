package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	o, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.addr != ":6379" || o.shards != 64 || o.server.MaxConns != 10000 || o.janitorInterval != time.Second {
		t.Fatalf("unexpected defaults: %+v", o)
	}
	if err := o.server.Validate(); err != nil {
		t.Fatalf("default server config is invalid: %v", err)
	}
}

func TestParseFlagsOverrides(t *testing.T) {
	o, err := parseFlags([]string{
		"-addr", "127.0.0.1:7000", "-shards", "8", "-max-conns", "5",
		"-idle-timeout", "30s", "-log-level", "debug", "-log-json",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.addr != "127.0.0.1:7000" || o.shards != 8 || o.server.MaxConns != 5 ||
		o.server.IdleTimeout != 30*time.Second || o.logLevel != "debug" || !o.logJSON {
		t.Fatalf("overrides not applied: %+v", o)
	}
}

func TestParseFlagsErrors(t *testing.T) {
	if _, err := parseFlags([]string{"-nope"}, io.Discard); err == nil {
		t.Fatal("unknown flag should fail")
	}
	if _, err := parseFlags([]string{"-h"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("-h should return flag.ErrHelp, got %v", err)
	}
}

func TestNewLogger(t *testing.T) {
	var buf bytes.Buffer
	log, err := newLogger(&buf, "warn", true)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("hidden")
	log.Warn("visible")
	out := buf.String()
	if strings.Contains(out, "hidden") || !strings.Contains(out, `"msg":"visible"`) {
		t.Fatalf("level filtering or JSON output wrong: %q", out)
	}

	if _, err := newLogger(io.Discard, "loud", false); err == nil {
		t.Fatal("invalid level should fail")
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	if err := run([]string{"-max-conns", "0"}, io.Discard); err == nil {
		t.Fatal("max-conns=0 should be rejected")
	}
	if err := run([]string{"-log-level", "loud"}, io.Discard); err == nil {
		t.Fatal("invalid log level should be rejected")
	}
}
