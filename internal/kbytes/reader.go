// Package kbytes provides the byte-level primitives for the Kafka wire protocol.
// It is the single place (de)serialization of integers, strings, bytes and arrays
// lives, so no other package reaches for encoding/binary directly (DRY).
//
// Only the fixed (non-"compact", non-tagged) encodings are implemented; the broker
// advertises pre-flexible API versions so the compact forms are never reached.
package kbytes

import (
	"encoding/binary"
	"errors"
)

// ErrShort is recorded on the Reader when a read runs past the buffer.
var ErrShort = errors.New("kbytes: short read")

// Reader decodes big-endian primitives from a byte slice. It uses a sticky error:
// once a read fails, subsequent reads return zero values and Err() reports the cause.
// Callers decode a whole message then check Err() once.
type Reader struct {
	buf []byte
	pos int
	err error
}

// NewReader wraps b. The slice is not copied.
func NewReader(b []byte) *Reader { return &Reader{buf: b} }

// Err returns the first error encountered, or nil.
func (r *Reader) Err() error { return r.err }

func (r *Reader) fail() {
	if r.err == nil {
		r.err = ErrShort
	}
}

func (r *Reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || r.pos+n > len(r.buf) {
		r.fail()
		return nil
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b
}

func (r *Reader) Int8() int8 {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return int8(b[0])
}

func (r *Reader) Int16() int16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(b))
}

func (r *Reader) Int32() int32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(b))
}

func (r *Reader) Int64() int64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

// Varint reads a zig-zag encoded signed varint (used inside record batches).
func (r *Reader) Varint() int64 {
	if r.err != nil {
		return 0
	}
	u, n := binary.Uvarint(r.buf[r.pos:])
	if n <= 0 {
		r.fail()
		return 0
	}
	r.pos += n
	return int64(u>>1) ^ -int64(u&1)
}

// String reads an INT16 length-prefixed string. A length of -1 yields "".
func (r *Reader) String() string {
	n := int(r.Int16())
	if n < 0 {
		return ""
	}
	b := r.take(n)
	return string(b)
}

// NullableString reads an INT16 length-prefixed string; -1 yields nil.
func (r *Reader) NullableString() *string {
	n := int(r.Int16())
	if n < 0 {
		return nil
	}
	b := r.take(n)
	s := string(b)
	return &s
}

// Bytes reads an INT32 length-prefixed byte slice (copied). -1 yields nil.
func (r *Reader) Bytes() []byte {
	n := int(r.Int32())
	if n < 0 {
		return nil
	}
	b := r.take(n)
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// ArrayLen reads an INT32 array length. A null array (-1) is reported as 0.
func (r *Reader) ArrayLen() int {
	n := int(r.Int32())
	if n < 0 {
		return 0
	}
	if n > len(r.buf)-r.pos { // guard against absurd lengths from malformed input
		r.fail()
		return 0
	}
	return n
}

// Raw returns the next n bytes verbatim without copying (used for opaque record sets).
func (r *Reader) Raw(n int) []byte { return r.take(n) }

// Remaining returns the unread tail without advancing.
func (r *Reader) Remaining() []byte {
	if r.err != nil {
		return nil
	}
	return r.buf[r.pos:]
}

// Pos reports the current read position.
func (r *Reader) Pos() int { return r.pos }
