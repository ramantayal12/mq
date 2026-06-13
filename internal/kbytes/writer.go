package kbytes

import "encoding/binary"

// Writer encodes big-endian primitives into a growable buffer.
type Writer struct{ buf []byte }

// NewWriter returns an empty Writer.
func NewWriter() *Writer { return &Writer{} }

// Len reports bytes written so far.
func (w *Writer) Len() int { return len(w.buf) }

func (w *Writer) Int8(v int8) { w.buf = append(w.buf, byte(v)) }
func (w *Writer) Int16(v int16) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
}
func (w *Writer) Int32(v int32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
}
func (w *Writer) Int64(v int64) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v))
}

// Bool writes a single byte 0/1.
func (w *Writer) Bool(v bool) {
	if v {
		w.Int8(1)
	} else {
		w.Int8(0)
	}
}

// String writes an INT16 length-prefixed string.
func (w *Writer) String(s string) {
	w.Int16(int16(len(s)))
	w.buf = append(w.buf, s...)
}

// NullableString writes an INT16 length-prefixed string; nil writes -1.
func (w *Writer) NullableString(s *string) {
	if s == nil {
		w.Int16(-1)
		return
	}
	w.String(*s)
}

// Bytes writes an INT32 length-prefixed byte slice; nil writes -1.
func (w *Writer) Bytes(b []byte) {
	if b == nil {
		w.Int32(-1)
		return
	}
	w.Int32(int32(len(b)))
	w.buf = append(w.buf, b...)
}

// ArrayLen writes an INT32 array length.
func (w *Writer) ArrayLen(n int) { w.Int32(int32(n)) }

// Raw appends bytes verbatim (no length prefix).
func (w *Writer) Raw(b []byte) { w.buf = append(w.buf, b...) }

// Finish returns the backing buffer.
func (w *Writer) Finish() []byte { return w.buf }

// PatchInt32 overwrites 4 bytes at off with v (used to backfill length prefixes).
func (w *Writer) PatchInt32(off int, v int32) {
	binary.BigEndian.PutUint32(w.buf[off:off+4], uint32(v))
}
