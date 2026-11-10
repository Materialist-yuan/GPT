package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

type slowReader struct{ r io.Reader }

func (r slowReader) Read(b []byte) (int, error) {
	if len(b) > 1 {
		b = b[:1]
	}
	return r.r.Read(b)
}

type slowWriter struct{ bytes.Buffer }

func (w *slowWriter) Write(b []byte) (int, error) {
	if len(b) > 2 {
		b = b[:2]
	}
	return w.Buffer.Write(b)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestSplitAndCoalescedFrames(t *testing.T) {
	frames := []Frame{{Opcode: Auth, Seq: 1, Body: []byte(`{"ticket":"abc"}`)}, {Opcode: Echo, Seq: 9, Body: []byte(`{"text":"你好"}`)}}
	var buf slowWriter
	for _, f := range frames {
		if err := WriteFrame(&buf, f); err != nil {
			t.Fatal(err)
		}
	}
	r := slowReader{bytes.NewReader(buf.Bytes())}
	for _, want := range frames {
		got, err := ReadFrame(r, DefaultMaxBody)
		if err != nil {
			t.Fatal(err)
		}
		if got.Opcode != want.Opcode || got.Seq != want.Seq || !bytes.Equal(got.Body, want.Body) {
			t.Fatalf("got %+v want %+v", got, want)
		}
	}
	if _, err := ReadFrame(r, DefaultMaxBody); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF got %v", err)
	}
}

func TestRejectInvalidHeaderBeforeBodyAllocation(t *testing.T) {
	for _, n := range []uint32{0, 11, ^uint32(0), DefaultMaxBody + 13} {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], n)
		if _, err := ReadFrame(bytes.NewReader(b[:]), DefaultMaxBody); !errors.Is(err, ErrInvalidFrame) {
			t.Errorf("size %d: %v", n, err)
		}
	}
	var good bytes.Buffer
	if err := WriteFrame(&good, Frame{Opcode: Ping, Seq: 1, Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{4, 6, 7} {
		b := append([]byte(nil), good.Bytes()...)
		b[index] = 255
		if _, err := ReadFrame(bytes.NewReader(b), DefaultMaxBody); !errors.Is(err, ErrInvalidFrame) {
			t.Errorf("index %d: %v", index, err)
		}
	}
	if _, err := ReadFrame(bytes.NewReader(good.Bytes()[:good.Len()-1]), DefaultMaxBody); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated body: %v", err)
	}
}

func TestZeroLengthWriterFails(t *testing.T) {
	if err := WriteFrame(zeroWriter{}, Frame{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("got %v", err)
	}
}
