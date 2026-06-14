package storage

import (
	"encoding/binary"
	"errors"
	"testing"

	"mq/internal/record"
)

// makeBatch builds a valid v2 batch with recordCount records (lastOffsetDelta =
// recordCount-1) and a payload to pad its size.
func makeBatch(recordCount int32, padding int) []byte {
	b := make([]byte, record.HeaderSize+padding)
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12))      // batchLength
	b[16] = 2                                                 // magic
	binary.BigEndian.PutUint32(b[23:], uint32(recordCount-1)) // lastOffsetDelta
	binary.BigEndian.PutUint32(b[57:], uint32(recordCount))   // recordCount
	record.RecomputeCRC(b)
	return b
}

func TestAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	off0, _ := l.Append(makeBatch(3, 10)) // offsets 0,1,2
	off1, _ := l.Append(makeBatch(2, 10)) // offsets 3,4
	if off0 != 0 || off1 != 3 {
		t.Fatalf("offsets: %d %d", off0, off1)
	}
	if l.LatestOffset() != 5 {
		t.Fatalf("latest=%d want 5", l.LatestOffset())
	}

	out, err := l.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var bases []int64
	record.Iterate(out, func(batch []byte) error {
		h, _ := record.ParseHeader(batch)
		bases = append(bases, h.BaseOffset)
		return nil
	})
	if len(bases) != 2 || bases[0] != 0 || bases[1] != 3 {
		t.Fatalf("bases=%v", bases)
	}

	// Read from a mid offset should return the batch covering it.
	out, _ = l.Read(3, 1<<20)
	h, _ := record.ParseHeader(out)
	if h.BaseOffset != 3 {
		t.Fatalf("read@3 base=%d", h.BaseOffset)
	}

	// Read past the end returns nothing.
	if out, _ := l.Read(5, 1<<20); out != nil {
		t.Fatalf("read past end returned %d bytes", len(out))
	}
}

// makeBatchTS builds a single-record batch stamped with firstTimestamp/maxTimestamp.
func makeBatchTS(ts int64) []byte {
	b := makeBatch(1, 0)
	binary.BigEndian.PutUint64(b[27:], uint64(ts)) // firstTimestamp
	binary.BigEndian.PutUint64(b[35:], uint64(ts)) // maxTimestamp
	record.RecomputeCRC(b)
	return b
}

func TestOffsetForTimestamp(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, DefaultConfig())
	defer l.Close()
	// offsets 0..2 at timestamps 100, 200, 300.
	for i, ts := range []int64{100, 200, 300} {
		off, _ := l.Append(makeBatchTS(ts))
		if off != int64(i) {
			t.Fatalf("append %d got offset %d", i, off)
		}
	}

	cases := []struct {
		ts        int64
		wantOff   int64
		wantFound bool
	}{
		{50, 0, true},   // before everything -> first batch
		{200, 1, true},  // exact match
		{250, 2, true},  // between -> next batch
		{300, 2, true},  // last exact
		{301, 0, false}, // after everything -> not found
	}
	for _, c := range cases {
		off, found := l.OffsetForTimestamp(c.ts)
		if found != c.wantFound || (found && off != c.wantOff) {
			t.Fatalf("OffsetForTimestamp(%d)=(%d,%v) want (%d,%v)", c.ts, off, found, c.wantOff, c.wantFound)
		}
	}
}

func TestReadOffsetOutOfRange(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, DefaultConfig())
	defer l.Close()
	l.Append(makeBatch(3, 10)) // offsets 0,1,2; nextOffset=3

	// Exactly at the high watermark: caught up, no data, no error.
	if out, err := l.Read(3, 1<<20); out != nil || err != nil {
		t.Fatalf("read@HWM: out=%d err=%v", len(out), err)
	}
	// Strictly above the high watermark: out of range.
	if _, err := l.Read(4, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read@4 err=%v, want ErrOffsetOutOfRange", err)
	}
}

func TestReadBelowEarliestOutOfRange(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{SegmentBytes: 100, IndexIntervalBytes: 1}
	l, _ := Open(dir, cfg)
	defer l.Close()
	for i := 0; i < 5; i++ {
		l.Append(makeBatch(1, 50)) // ~111 bytes each => rolls every append; offsets 0..4
	}
	// Drop the oldest segments so the earliest retained offset moves above 0.
	if err := l.EnforceRetention(0, 50); err != nil {
		t.Fatal(err)
	}
	earliest := l.EarliestOffset()
	if earliest == 0 {
		t.Fatalf("retention deleted nothing; earliest=%d", earliest)
	}
	// A read below the earliest retained offset is out of range (retention ate it).
	if _, err := l.Read(0, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read@0 err=%v, want ErrOffsetOutOfRange", err)
	}
	// A read at the new earliest still works.
	if out, err := l.Read(earliest, 1<<20); err != nil || out == nil {
		t.Fatalf("read@earliest: out=%d err=%v", len(out), err)
	}
}

func TestSegmentRoll(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{SegmentBytes: 200, IndexIntervalBytes: 1}
	l, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// each batch ~ 61+50 = 111 bytes, so the 2nd append rolls.
	for i := 0; i < 5; i++ {
		if _, err := l.Append(makeBatch(1, 50)); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(l.segments))
	}
	// Reads across the first offset still work.
	out, _ := l.Read(0, 1<<20)
	h, _ := record.ParseHeader(out)
	if h.BaseOffset != 0 {
		t.Fatalf("base=%d", h.BaseOffset)
	}
	// And a read landing in a later segment.
	out, _ = l.Read(4, 1<<20)
	h, _ = record.ParseHeader(out)
	if h.BaseOffset != 4 {
		t.Fatalf("base=%d want 4", h.BaseOffset)
	}
}

func TestRecoveryAfterReopen(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, DefaultConfig())
	l.Append(makeBatch(3, 10))
	l.Append(makeBatch(2, 10))
	l.Flush()
	l.Close()

	l2, err := Open(dir, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.LatestOffset() != 5 {
		t.Fatalf("recovered latest=%d want 5", l2.LatestOffset())
	}
	off, _ := l2.Append(makeBatch(1, 10))
	if off != 5 {
		t.Fatalf("post-recovery append offset=%d want 5", off)
	}
}

func TestRecoveryTruncatesPartialTail(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, DefaultConfig())
	l.Append(makeBatch(2, 10))
	good := l.active.logSize
	// Simulate a torn write: append garbage shorter than a header.
	l.active.logFile.WriteAt([]byte{0, 1, 2}, int64(good))
	l.active.logSize += 3
	l.Flush()
	l.Close()

	l2, err := Open(dir, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.active.logSize != good {
		t.Fatalf("expected truncation to %d, got %d", good, l2.active.logSize)
	}
	if l2.LatestOffset() != 2 {
		t.Fatalf("latest=%d want 2", l2.LatestOffset())
	}
}

func TestFloorPosition(t *testing.T) {
	idx := []indexEntry{{0, 0}, {10, 500}, {20, 1000}}
	if p := floorPosition(idx, 15); p != 500 {
		t.Fatalf("floor(15)=%d want 500", p)
	}
	if p := floorPosition(idx, 25); p != 1000 {
		t.Fatalf("floor(25)=%d want 1000", p)
	}
	if p := floorPosition(idx, 0); p != 0 {
		t.Fatalf("floor(0)=%d want 0", p)
	}
}

// TestHighWatermarkDefaultTracksLEO verifies that without replication every append
// commits immediately: the high watermark always equals the log end offset.
func TestHighWatermarkDefaultTracksLEO(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, DefaultConfig())
	defer l.Close()
	if l.HighWatermark() != 0 {
		t.Fatalf("fresh hwm=%d want 0", l.HighWatermark())
	}
	l.Append(makeBatch(3, 0)) // 0,1,2
	l.Append(makeBatch(2, 0)) // 3,4
	if l.HighWatermark() != l.LatestOffset() || l.HighWatermark() != 5 {
		t.Fatalf("hwm=%d leo=%d want both 5", l.HighWatermark(), l.LatestOffset())
	}
}

// TestHoldHighWatermark verifies replication mode: appends advance only the LEO, and the
// HWM moves only via SetHighWatermark, clamped to [current, LEO] and monotonic.
func TestHoldHighWatermark(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, DefaultConfig())
	defer l.Close()
	l.HoldHighWatermark()

	l.Append(makeBatch(3, 0)) // LEO 3
	l.Append(makeBatch(2, 0)) // LEO 5
	if l.HighWatermark() != 0 {
		t.Fatalf("held hwm=%d want 0", l.HighWatermark())
	}
	l.SetHighWatermark(3)
	if l.HighWatermark() != 3 {
		t.Fatalf("hwm=%d want 3", l.HighWatermark())
	}
	l.SetHighWatermark(2) // never moves backward
	if l.HighWatermark() != 3 {
		t.Fatalf("hwm went backward to %d", l.HighWatermark())
	}
	l.SetHighWatermark(100) // never exceeds the LEO
	if l.HighWatermark() != 5 {
		t.Fatalf("hwm=%d want clamped to LEO 5", l.HighWatermark())
	}
}

// TestAppendReplica verifies a follower preserves the leader's offsets, skips duplicates,
// rejects gaps, and never advances the HWM on its own.
func TestAppendReplica(t *testing.T) {
	dir := t.TempDir()
	leader, _ := Open(t.TempDir(), DefaultConfig())
	defer leader.Close()
	follower, _ := Open(dir, DefaultConfig())
	defer follower.Close()
	follower.HoldHighWatermark()

	// Leader assigns offsets; capture its on-disk batches.
	leader.Append(makeBatch(3, 5)) // 0,1,2
	leader.Append(makeBatch(2, 5)) // 3,4
	set, _ := leader.Read(0, 1<<20)

	var batches [][]byte
	record.Iterate(set, func(b []byte) error {
		cp := make([]byte, len(b))
		copy(cp, b)
		batches = append(batches, cp)
		return nil
	})
	if len(batches) != 2 {
		t.Fatalf("got %d batches", len(batches))
	}

	leo, err := follower.AppendReplica(batches[0])
	if err != nil || leo != 3 {
		t.Fatalf("replica append1 leo=%d err=%v", leo, err)
	}
	// Duplicate is a no-op.
	if leo, err := follower.AppendReplica(batches[0]); err != nil || leo != 3 {
		t.Fatalf("duplicate replica append leo=%d err=%v", leo, err)
	}
	// Gap is rejected (skipping batches[1] base 3 is fine; jumping ahead is not).
	gap := makeBatch(1, 0)
	binary.BigEndian.PutUint64(gap[0:], 9) // base 9, far ahead
	record.RecomputeCRC(gap)
	if _, err := follower.AppendReplica(gap); err == nil {
		t.Fatal("expected gap rejection")
	}
	if leo, err := follower.AppendReplica(batches[1]); err != nil || leo != 5 {
		t.Fatalf("replica append2 leo=%d err=%v", leo, err)
	}
	if follower.HighWatermark() != 0 {
		t.Fatalf("follower hwm=%d want 0 (only SetHighWatermark advances it)", follower.HighWatermark())
	}

	// Offsets match the leader's exactly.
	out, _ := follower.Read(0, 1<<20)
	var bases []int64
	record.Iterate(out, func(b []byte) error {
		h, _ := record.ParseHeader(b)
		bases = append(bases, h.BaseOffset)
		return nil
	})
	if len(bases) != 2 || bases[0] != 0 || bases[1] != 3 {
		t.Fatalf("follower bases=%v want [0 3]", bases)
	}
}
