// Package record parses and patches the header of a Kafka RecordBatch (message
// format v2). The broker treats the records section as an opaque blob (it may be
// compressed); only the fixed 61-byte header is inspected, so compression is
// transparent to mq.
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// HeaderSize is the fixed size of a RecordBatch v2 header in bytes.
const HeaderSize = 61

// Field offsets within a RecordBatch v2 (from the start of the batch).
const (
	offBaseOffset      = 0  // int64
	offBatchLength     = 8  // int32 (bytes following this field)
	offLeaderEpoch     = 12 // int32
	offMagic           = 16 // int8
	offCRC             = 17 // uint32 (CRC32-C over bytes [21:])
	offAttributes      = 21 // int16
	offLastOffsetDelta = 23 // int32
	offFirstTimestamp  = 27 // int64
	offMaxTimestamp    = 35 // int64
	offRecordCount     = 57 // int32
)

var (
	// ErrTooShort means the slice is smaller than a v2 header.
	ErrTooShort = errors.New("record: batch shorter than header")
	// ErrBadMagic means the magic byte is not 2.
	ErrBadMagic = errors.New("record: unsupported magic (want v2)")

	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// BatchHeader holds the header fields mq needs to manage offsets.
type BatchHeader struct {
	BaseOffset      int64
	BatchLength     int32 // bytes after offBatchLength; full batch on disk = 12 + BatchLength
	LastOffsetDelta int32
	FirstTimestamp  int64 // timestamp of the first record (ms since epoch)
	MaxTimestamp    int64 // largest timestamp in the batch (ms since epoch)
	RecordCount     int32
	Magic           int8
}

// TotalSize returns the full on-disk size of the batch this header describes.
func (h BatchHeader) TotalSize() int { return 12 + int(h.BatchLength) }

// LastOffset returns BaseOffset + LastOffsetDelta.
func (h BatchHeader) LastOffset() int64 { return h.BaseOffset + int64(h.LastOffsetDelta) }

// ParseHeader decodes the header at the start of b and validates magic==2.
func ParseHeader(b []byte) (BatchHeader, error) {
	if len(b) < HeaderSize {
		return BatchHeader{}, ErrTooShort
	}
	h := BatchHeader{
		BaseOffset:      int64(binary.BigEndian.Uint64(b[offBaseOffset:])),
		BatchLength:     int32(binary.BigEndian.Uint32(b[offBatchLength:])),
		Magic:           int8(b[offMagic]),
		LastOffsetDelta: int32(binary.BigEndian.Uint32(b[offLastOffsetDelta:])),
		FirstTimestamp:  int64(binary.BigEndian.Uint64(b[offFirstTimestamp:])),
		MaxTimestamp:    int64(binary.BigEndian.Uint64(b[offMaxTimestamp:])),
		RecordCount:     int32(binary.BigEndian.Uint32(b[offRecordCount:])),
	}
	if h.Magic != 2 {
		return h, ErrBadMagic
	}
	return h, nil
}

// SetBaseOffset patches the baseOffset field in place.
func SetBaseOffset(b []byte, off int64) {
	binary.BigEndian.PutUint64(b[offBaseOffset:], uint64(off))
}

// RecomputeCRC recomputes the CRC32-C over b[21:] and writes it at the crc offset.
// Must be called after any header mutation (e.g. SetBaseOffset) because the CRC
// covers the attributes field onward.
func RecomputeCRC(b []byte) {
	crc := crc32.Checksum(b[offAttributes:], castagnoli)
	binary.BigEndian.PutUint32(b[offCRC:], crc)
}

// Iterate walks a concatenated record-set, calling fn with each batch sub-slice.
// It stops at the first malformed/truncated batch and returns the bytes consumed.
func Iterate(set []byte, fn func(batch []byte) error) (consumed int, err error) {
	for consumed < len(set) {
		h, perr := ParseHeader(set[consumed:])
		if perr != nil {
			return consumed, perr
		}
		size := h.TotalSize()
		if consumed+size > len(set) {
			return consumed, ErrTooShort // truncated trailing batch
		}
		if err := fn(set[consumed : consumed+size]); err != nil {
			return consumed, err
		}
		consumed += size
	}
	return consumed, nil
}
