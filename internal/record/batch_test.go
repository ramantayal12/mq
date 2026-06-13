package record

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// buildBatch constructs a minimal valid v2 batch (header + n bytes of opaque
// "records") with the given baseOffset, lastOffsetDelta and recordCount.
func buildBatch(baseOffset int64, lastDelta, recordCount int32, payload []byte) []byte {
	b := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint64(b[offBaseOffset:], uint64(baseOffset))
	binary.BigEndian.PutUint32(b[offBatchLength:], uint32(len(b)-12))
	b[offMagic] = 2
	binary.BigEndian.PutUint32(b[offLastOffsetDelta:], uint32(lastDelta))
	binary.BigEndian.PutUint32(b[offRecordCount:], uint32(recordCount))
	copy(b[HeaderSize:], payload)
	RecomputeCRC(b)
	return b
}

func TestParseAndPatch(t *testing.T) {
	b := buildBatch(0, 4, 5, []byte("opaque-records"))
	h, err := ParseHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.RecordCount != 5 || h.LastOffsetDelta != 4 {
		t.Fatalf("header=%+v", h)
	}
	if h.TotalSize() != len(b) {
		t.Fatalf("TotalSize=%d len=%d", h.TotalSize(), len(b))
	}

	SetBaseOffset(b, 1000)
	RecomputeCRC(b)
	h, _ = ParseHeader(b)
	if h.BaseOffset != 1000 || h.LastOffset() != 1004 {
		t.Fatalf("after patch: base=%d last=%d", h.BaseOffset, h.LastOffset())
	}
	// CRC must validate over [21:]
	got := crc32.Checksum(b[offAttributes:], castagnoli)
	want := binary.BigEndian.Uint32(b[offCRC:])
	if got != want {
		t.Fatalf("crc mismatch got=%x want=%x", got, want)
	}
}

func TestIterateMultipleBatches(t *testing.T) {
	b1 := buildBatch(0, 0, 1, []byte("a"))
	b2 := buildBatch(0, 2, 3, []byte("bbb"))
	set := append(append([]byte{}, b1...), b2...)

	var counts []int32
	consumed, err := Iterate(set, func(batch []byte) error {
		h, _ := ParseHeader(batch)
		counts = append(counts, h.RecordCount)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(set) {
		t.Fatalf("consumed=%d want=%d", consumed, len(set))
	}
	if len(counts) != 2 || counts[0] != 1 || counts[1] != 3 {
		t.Fatalf("counts=%v", counts)
	}
}

func TestTruncatedTail(t *testing.T) {
	b := buildBatch(0, 0, 1, []byte("payload"))
	set := b[:len(b)-3] // chop the tail
	_, err := Iterate(set, func([]byte) error { return nil })
	if err == nil {
		t.Fatal("expected truncation error")
	}
}

func TestBadMagic(t *testing.T) {
	b := buildBatch(0, 0, 1, nil)
	b[offMagic] = 1
	if _, err := ParseHeader(b); err != ErrBadMagic {
		t.Fatalf("want ErrBadMagic got %v", err)
	}
}
