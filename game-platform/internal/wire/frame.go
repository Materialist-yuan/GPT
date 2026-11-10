// Package wire implements the version 1 game gateway framing protocol.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	Version              = 1
	HeaderSize           = 16
	DefaultMaxBody       = 64 * 1024
	Auth           uint8 = 1
	AuthOK         uint8 = 2
	Ping           uint8 = 3
	Pong           uint8 = 4
	Echo           uint8 = 5
	EchoOK         uint8 = 6
	Error          uint8 = 255
)

var ErrInvalidFrame = errors.New("invalid frame")

type Frame struct {
	Opcode uint8
	Seq    uint64
	Body   []byte
}

// ReadFrame never allocates based on a peer's size field before validating it.
// The size prefix counts all bytes after the prefix, including the 12-byte header.
func ReadFrame(r io.Reader, maxBody int) (Frame, error) {
	var h [HeaderSize]byte
	if _, err := io.ReadFull(r, h[:4]); err != nil {
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(h[:4])
	if maxBody < 0 || n < HeaderSize-4 || uint64(n) > uint64(maxBody)+HeaderSize-4 {
		return Frame{}, fmt.Errorf("%w: length", ErrInvalidFrame)
	}
	if _, err := io.ReadFull(r, h[4:]); err != nil {
		return Frame{}, err
	}
	if h[4] != Version || binary.BigEndian.Uint16(h[6:8]) != 0 {
		return Frame{}, fmt.Errorf("%w: version or reserved bits", ErrInvalidFrame)
	}
	f := Frame{Opcode: h[5], Seq: binary.BigEndian.Uint64(h[8:]), Body: make([]byte, int(n)-(HeaderSize-4))}
	if _, err := io.ReadFull(r, f.Body); err != nil {
		return Frame{}, err
	}
	return f, nil
}

// WriteFrame handles writers that accept only part of each buffer.
func WriteFrame(w io.Writer, f Frame) error {
	if uint64(len(f.Body)) > uint64(^uint32(0))-(HeaderSize-4) {
		return fmt.Errorf("%w: length", ErrInvalidFrame)
	}
	var h [HeaderSize]byte
	binary.BigEndian.PutUint32(h[:4], uint32(HeaderSize-4+len(f.Body)))
	h[4], h[5] = Version, f.Opcode
	binary.BigEndian.PutUint64(h[8:], f.Seq)
	if err := writeAll(w, h[:]); err != nil {
		return err
	}
	return writeAll(w, f.Body)
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
