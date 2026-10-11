package protocol

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

func argsToStrings(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"resp array", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", []string{"GET", "key"}},
		{"binary safe payload", "*2\r\n$3\r\nSET\r\n$6\r\na\r\nb\x00c\r\n", []string{"SET", "a\r\nb\x00c"}},
		{"empty bulk", "*2\r\n$3\r\nSET\r\n$0\r\n\r\n", []string{"SET", ""}},
		{"inline", "SET  foo   bar\r\n", []string{"SET", "foo", "bar"}},
		{"inline with bare LF", "PING\n", []string{"PING"}},
		{"empty line is ignored", "\r\n", nil},
		{"empty array is ignored", "*0\r\n", nil},
		{"null array is ignored", "*-1\r\n", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input), Limits{})
			args, err := r.ReadCommand()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := argsToStrings(args); !slices.Equal(got, tt.want) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadCommandPipelined(t *testing.T) {
	r := NewReader(strings.NewReader("PING\r\n*1\r\n$4\r\nPING\r\n"), Limits{})
	for i := range 2 {
		args, err := r.ReadCommand()
		if err != nil || len(args) != 1 || string(args[0]) != "PING" {
			t.Fatalf("command %d: got %q, %v", i, args, err)
		}
	}
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after last command, got %v", err)
	}
}

func TestReadCommandSurvivesOneByteReads(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n"
	r := NewReader(iotest.OneByteReader(strings.NewReader(input)), Limits{})
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if got := argsToStrings(args); !slices.Equal(got, []string{"SET", "k", "hello"}) {
		t.Fatalf("got %q", got)
	}
}

func TestReadCommandLargeBulk(t *testing.T) {
	payload := strings.Repeat("x", 200<<10) // exceeds eagerAllocLimit
	input := "*2\r\n$3\r\nSET\r\n$" + "204800" + "\r\n" + payload + "\r\n"
	r := NewReader(strings.NewReader(input), Limits{MaxBulkLen: 1 << 20})
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 2 || string(args[1]) != payload {
		t.Fatalf("large payload not read intact (got %d args)", len(args))
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		limits Limits
	}{
		{"bad array length", "*abc\r\n", Limits{}},
		{"too many args", "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", Limits{MaxArgs: 2}},
		{"element is not a bulk string", "*1\r\n:5\r\n", Limits{}},
		{"missing element header", "*1\r\n\r\n", Limits{}},
		{"negative bulk length", "*1\r\n$-5\r\n", Limits{}},
		{"bulk too large", "*1\r\n$100\r\n", Limits{MaxBulkLen: 10}},
		{"bad bulk length", "*1\r\n$x\r\n", Limits{}},
		{"bulk missing CRLF", "*1\r\n$3\r\nabcde\r\n", Limits{}},
		{"line too long", strings.Repeat("a", readBufSize+10) + "\r\n", Limits{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tt.input), tt.limits).ReadCommand()
			var pe *Error
			if !errors.As(err, &pe) {
				t.Fatalf("got %v (%T), want *Error", err, err)
			}
		})
	}
}

func TestReadCommandIOErrors(t *testing.T) {
	t.Run("clean EOF", func(t *testing.T) {
		_, err := NewReader(strings.NewReader(""), Limits{}).ReadCommand()
		if !errors.Is(err, io.EOF) {
			t.Fatalf("got %v, want io.EOF", err)
		}
	})
	t.Run("truncated command", func(t *testing.T) {
		_, err := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n"), Limits{}).ReadCommand()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("truncated bulk payload", func(t *testing.T) {
		_, err := NewReader(strings.NewReader("*1\r\n$10\r\nabc"), Limits{}).ReadCommand()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

func TestWriter(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Writer)
		want  string
	}{
		{"simple", func(w *Writer) { w.Simple("OK") }, "+OK\r\n"},
		{"error", func(w *Writer) { w.Error("ERR boom") }, "-ERR boom\r\n"},
		{"integer", func(w *Writer) { w.Integer(-42) }, ":-42\r\n"},
		{"bulk", func(w *Writer) { w.Bulk([]byte("hello")) }, "$5\r\nhello\r\n"},
		{"empty bulk", func(w *Writer) { w.Bulk(nil) }, "$0\r\n\r\n"},
		{"binary bulk", func(w *Writer) { w.Bulk([]byte("a\r\nb")) }, "$4\r\na\r\nb\r\n"},
		{"null", func(w *Writer) { w.Null() }, "$-1\r\n"},
		{"array", func(w *Writer) { w.Array(2); w.Integer(1); w.Null() }, "*2\r\n:1\r\n$-1\r\n"},
		{"CRLF in error cannot split the reply", func(w *Writer) { w.Error("ERR a\r\n+OK") }, "-ERR a  +OK\r\n"},
		{"CRLF in simple string cannot split the reply", func(w *Writer) { w.Simple("a\nb") }, "+a b\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			tt.write(w)
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestWriterReportsErrorsOnFlush(t *testing.T) {
	w := NewWriter(failingWriter{})
	w.Simple("OK")
	if err := w.Flush(); err == nil {
		t.Fatal("expected Flush to report the write error")
	}
}

// FuzzReadCommand checks that arbitrary input never panics, never loops
// forever and never yields commands that violate the configured limits.
// Run with: go test -fuzz=FuzzReadCommand ./internal/protocol
func FuzzReadCommand(f *testing.F) {
	seeds := []string{
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		"PING\r\n",
		"*1\r\n$-1\r\n",
		"*99999999999\r\n",
		"$5\r\nhello\r\n",
		"*2\r\n$3\r\nSET\r\n$3\r\nab",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	limits := Limits{MaxBulkLen: 1024, MaxArgs: 16}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data), limits)
		for range len(data) + 1 { // each successful read consumes at least one byte
			args, err := r.ReadCommand()
			if err != nil {
				return
			}
			if len(args) > limits.MaxArgs {
				t.Fatalf("got %d args, limit is %d", len(args), limits.MaxArgs)
			}
			// RESP arguments are bounded by MaxBulkLen; inline arguments are
			// bounded by the maximum line length instead.
			maxArg := max(limits.MaxBulkLen, readBufSize)
			for _, a := range args {
				if len(a) > maxArg {
					t.Fatalf("argument of %d bytes exceeds limit of %d", len(a), maxArg)
				}
			}
		}
	})
}
