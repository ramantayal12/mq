package object

// Unit tests for the read-path helpers — pure functions over in-memory batch bytes, no MinIO.

import (
	"encoding/binary"
	"testing"

	"mq/internal/record"
)

// tBatch builds a valid v2 batch based at base with count records, max timestamp maxTs, and
// some padding. Mirrors the integration test's makeBatch but with offset/timestamp control.
func tBatch(base int64, count int32, maxTs int64, padding int) []byte {
	b := make([]byte, record.HeaderSize+padding)
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12)) // BatchLength
	b[16] = 2                                            // magic
	binary.BigEndian.PutUint32(b[23:], uint32(count-1))  // LastOffsetDelta
	binary.BigEndian.PutUint64(b[35:], uint64(maxTs))    // MaxTimestamp
	binary.BigEndian.PutUint32(b[57:], uint32(count))    // RecordCount
	record.SetBaseOffset(b, base)
	record.RecomputeCRC(b)
	return b
}

func basesOf(t *testing.T, data []byte) []int64 {
	t.Helper()
	var got []int64
	for pos := 0; pos < len(data); {
		h, err := record.ParseHeader(data[pos:])
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		got = append(got, h.BaseOffset)
		pos += h.TotalSize()
	}
	return got
}

func TestSliceBatches(t *testing.T) {
	// [0,3) [3,5) [5,6), each batch a different size.
	data := append(append(tBatch(0, 3, 10, 8), tBatch(3, 2, 20, 24)...), tBatch(5, 1, 30, 8)...)

	cases := []struct {
		want   int64
		expect []int64
	}{
		{0, []int64{0, 3, 5}},
		{3, []int64{3, 5}},
		{4, []int64{3, 5}}, // batch [3,5) covers offset 4
		{5, []int64{5}},
	}
	for _, c := range cases {
		out, err := sliceBatches(data, c.want, 1<<20)
		if err != nil {
			t.Fatalf("want=%d: %v", c.want, err)
		}
		got := basesOf(t, out)
		if len(got) != len(c.expect) {
			t.Fatalf("want=%d bases=%v expect %v", c.want, got, c.expect)
		}
		for i := range c.expect {
			if got[i] != c.expect[i] {
				t.Fatalf("want=%d bases=%v expect %v", c.want, got, c.expect)
			}
		}
	}

	// maxBytes returns at least one whole batch even when it exceeds the limit.
	out, err := sliceBatches(data, 0, 1)
	if err != nil {
		t.Fatalf("maxBytes: %v", err)
	}
	if bases := basesOf(t, out); len(bases) != 1 || bases[0] != 0 {
		t.Fatalf("maxBytes=1 bases=%v want [0]", bases)
	}
}

func TestScanTimestamp(t *testing.T) {
	data := append(append(tBatch(0, 3, 10, 8), tBatch(3, 2, 20, 8)...), tBatch(5, 1, 30, 8)...)
	cases := []struct {
		ts    int64
		off   int64
		found bool
	}{
		{5, 0, true},
		{10, 0, true},
		{15, 3, true},
		{30, 5, true},
		{31, 0, false},
	}
	for _, c := range cases {
		off, ok := scanTimestamp(data, c.ts)
		if ok != c.found || (ok && off != c.off) {
			t.Fatalf("ts=%d got (%d,%v) want (%d,%v)", c.ts, off, ok, c.off, c.found)
		}
	}
}

func TestIndexRefFor(t *testing.T) {
	ix := newIndex(NewMemIndexStore())
	ix.commit("t", 0, SegmentRef{Key: "a", BaseOffset: 0, NextOffset: 5, Position: 0, Length: 100})
	ix.commit("t", 0, SegmentRef{Key: "b", BaseOffset: 5, NextOffset: 8, Position: 0, Length: 60})

	cases := []struct {
		offset  int64
		wantKey string
		found   bool
	}{
		{0, "a", true},
		{4, "a", true},
		{5, "b", true},
		{7, "b", true},
		{8, "", false},  // == NextOffset of last ref
		{-1, "", false}, // below everything
	}
	for _, c := range cases {
		ref, ok := ix.refFor("t", 0, c.offset)
		if ok != c.found || ref.Key != c.wantKey {
			t.Fatalf("offset=%d got (%q,%v) want (%q,%v)", c.offset, ref.Key, ok, c.wantKey, c.found)
		}
	}
}
