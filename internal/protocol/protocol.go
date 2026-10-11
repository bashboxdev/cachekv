// Package protocol implements the subset of the Redis serialization protocol
// (RESP2) that CacheKV needs: parsing client commands and encoding replies.
//
// The package knows nothing about the store or about individual commands, so
// it can be tested, fuzzed and reused in isolation.
package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Default limits protect the server against hostile or buggy clients.
const (
	// DefaultMaxBulkLen is the largest accepted bulk string (command argument).
	DefaultMaxBulkLen = 1 << 20 // 1 MiB
	// DefaultMaxArgs is the largest accepted number of arguments per command.
	DefaultMaxArgs = 1024

	// readBufSize is also the maximum length of a single protocol line,
	// which bounds inline commands and RESP headers.
	readBufSize = 4096
	// eagerAllocLimit is the largest bulk payload allocated up-front. Larger
	// payloads grow as bytes actually arrive, so a client cannot reserve
	// memory by merely announcing a huge length.
	eagerAllocLimit = 64 << 10
)

// Error reports malformed client input. The connection should be closed after
// sending the message to the client.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

func protoErr(format string, args ...any) error {
	return &Error{Msg: fmt.Sprintf(format, args...)}
}

// Limits configures a Reader. Zero values select the defaults.
type Limits struct {
	MaxBulkLen int
	MaxArgs    int
}

// Reader parses client commands from a byte stream.
type Reader struct {
	br     *bufio.Reader
	limits Limits
}

// NewReader returns a Reader that reads from r.
func NewReader(r io.Reader, l Limits) *Reader {
	if l.MaxBulkLen <= 0 {
		l.MaxBulkLen = DefaultMaxBulkLen
	}
	if l.MaxArgs <= 0 {
		l.MaxArgs = DefaultMaxArgs
	}
	return &Reader{br: bufio.NewReaderSize(r, readBufSize), limits: l}
}

// Buffered returns the number of bytes that can be read without blocking.
// Servers use it to detect pipelined commands and batch their replies.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads one command and returns its arguments (the first element
// is the command name). Both RESP arrays of bulk strings and space-separated
// inline commands (handy for telnet/nc) are accepted. An empty line yields
// (nil, nil) and should simply be ignored by the caller.
//
// I/O errors, including io.EOF, are returned unchanged; malformed input is
// reported as *Error.
func (r *Reader) ReadCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] != '*' {
		return splitInline(line), nil
	}

	n, err := parseLen(line[1:])
	if err != nil {
		return nil, protoErr("invalid multibulk length")
	}
	if n <= 0 {
		return nil, nil // null or empty array: nothing to execute
	}
	if n > r.limits.MaxArgs {
		return nil, protoErr("too many arguments (max %d)", r.limits.MaxArgs)
	}

	args := make([][]byte, 0, n)
	for range n {
		arg, err := r.readBulk()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func (r *Reader) readBulk() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, unexpectedEOF(err)
	}
	if len(line) == 0 || line[0] != '$' {
		got := "end of line"
		if len(line) > 0 {
			got = fmt.Sprintf("'%c'", line[0])
		}
		return nil, protoErr("expected '$', got %s", got)
	}
	size, err := parseLen(line[1:])
	if err != nil || size < 0 || size > r.limits.MaxBulkLen {
		return nil, protoErr("invalid bulk length")
	}

	// Payload plus the trailing CRLF.
	var buf []byte
	if size+2 <= eagerAllocLimit {
		buf = make([]byte, size+2)
		_, err = io.ReadFull(r.br, buf)
	} else {
		var b bytes.Buffer
		_, err = io.CopyN(&b, r.br, int64(size)+2)
		buf = b.Bytes()
	}
	if err != nil {
		return nil, unexpectedEOF(err)
	}
	if buf[size] != '\r' || buf[size+1] != '\n' {
		return nil, protoErr("bulk string is not terminated by CRLF")
	}
	return buf[:size], nil
}

// readLine returns one line without its terminator. The result is only valid
// until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	switch {
	case errors.Is(err, bufio.ErrBufferFull):
		return nil, protoErr("line too long (max %d bytes)", readBufSize)
	case err != nil:
		return nil, err
	}
	line = line[:len(line)-1]
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func parseLen(b []byte) (int, error) {
	n, err := strconv.ParseInt(string(b), 10, 32)
	return int(n), err
}

// splitInline splits an inline command on ASCII whitespace. Quoting is not
// supported. Arguments are copied because line is only valid until the next read.
func splitInline(line []byte) [][]byte {
	fields := bytes.Fields(line)
	args := make([][]byte, len(fields))
	for i, f := range fields {
		args[i] = append([]byte(nil), f...)
	}
	return args
}

// Writer encodes RESP replies into a buffer. Write errors are sticky (a
// property of bufio.Writer) and are reported by Flush, so callers can chain
// writes without checking each one.
type Writer struct {
	bw      *bufio.Writer
	scratch [24]byte
}

// NewWriter returns a Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

// Flush sends all buffered replies and returns the first write error, if any.
func (w *Writer) Flush() error { return w.bw.Flush() }

// The helpers below deliberately discard errors: bufio.Writer remembers the
// first failure and Flush returns it.
func (w *Writer) putByte(c byte)     { _ = w.bw.WriteByte(c) }
func (w *Writer) putString(s string) { _, _ = w.bw.WriteString(s) }
func (w *Writer) putBytes(b []byte)  { _, _ = w.bw.Write(b) }

// putInt writes n in decimal followed by CRLF.
func (w *Writer) putInt(n int64) {
	w.putBytes(strconv.AppendInt(w.scratch[:0], n, 10))
	w.putString("\r\n")
}

// Simple writes a simple string reply, such as +OK.
func (w *Writer) Simple(s string) {
	w.putByte('+')
	w.putLine(s)
}

// Error writes an error reply. By convention msg starts with an upper-case
// error kind, for example "ERR unknown command".
func (w *Writer) Error(msg string) {
	w.putByte('-')
	w.putLine(msg)
}

// Integer writes an integer reply.
func (w *Writer) Integer(n int64) {
	w.putByte(':')
	w.putInt(n)
}

// Bulk writes a binary-safe bulk string reply.
func (w *Writer) Bulk(b []byte) {
	w.putByte('$')
	w.putInt(int64(len(b)))
	w.putBytes(b)
	w.putString("\r\n")
}

// Null writes the RESP2 null bulk string.
func (w *Writer) Null() { w.putString("$-1\r\n") }

// Array writes an array header; the caller must then write n elements.
func (w *Writer) Array(n int) {
	w.putByte('*')
	w.putInt(int64(n))
}

// putLine writes s followed by CRLF, replacing any embedded CR/LF so a reply
// can never be split or forged by user-controlled text.
func (w *Writer) putLine(s string) {
	for i := range len(s) {
		c := s[i]
		if c == '\r' || c == '\n' {
			c = ' '
		}
		w.putByte(c)
	}
	w.putString("\r\n")
}
