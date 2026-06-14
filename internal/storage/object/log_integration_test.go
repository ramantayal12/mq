//go:build integration

// Write-path integration test for the object backend. Needs the docker-compose MinIO up:
//
//	docker compose up -d minio createbuckets
//	go test -tags=integration ./internal/storage/object/ -run ObjectLog
//
// It exercises the AutoMQ-style flow end to end: per-partition ObjectLogs append through one
// shared node Storage, a flush batches every partition into a single stream-set object, the
// index records each partition's offset range, and a restart recovers un-uploaded data from
// the WAL.
package object

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"mq/internal/record"
	"mq/internal/storage"
)

// makeBatch builds a valid v2 batch with recordCount records and some padding (mirrors the
// storage package's test helper).
func makeBatch(recordCount int32, padding int) []byte {
	b := make([]byte, record.HeaderSize+padding)
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12))
	b[16] = 2
	binary.BigEndian.PutUint32(b[23:], uint32(recordCount-1))
	binary.BigEndian.PutUint32(b[57:], uint32(recordCount))
	record.RecomputeCRC(b)
	return b
}

// newTestStorage builds a node Storage over the compose MinIO with a unique node id (so object
// keys don't collide across runs) and auto-upload disabled (only explicit Flush uploads).
func newTestStorage(t *testing.T, walDir string, idx IndexStore) (*Storage, ObjectStore, string) {
	t.Helper()
	store, err := NewMinIO(testConfig())
	if err != nil {
		t.Fatalf("NewMinIO: %v", err)
	}
	nodeID := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	s, err := NewStorage(store, idx, StorageConfig{
		NodeID:      nodeID,
		WALDir:      walDir,
		UploadBytes: 0,           // no size trigger
		UploadMS:    60 * 60_000, // effectively no time trigger; tests flush explicitly
	})
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() {
		keys, _ := store.List(context.Background(), "data/sso/"+nodeID+"/")
		for _, k := range keys {
			_ = store.Delete(context.Background(), k)
		}
	})
	return s, store, nodeID
}

func TestObjectLogWritePath(t *testing.T) {
	idx := NewMemIndexStore()
	s, store, _ := newTestStorage(t, t.TempDir(), idx)

	p0, err := s.Log("t", 0)
	if err != nil {
		t.Fatalf("Log p0: %v", err)
	}
	p1, err := s.Log("t", 1)
	if err != nil {
		t.Fatalf("Log p1: %v", err)
	}

	// p0 gets offsets 0,1,2 then 3,4 (two batches); p1 gets 0,1.
	if b, _ := p0.Append(makeBatch(3, 8)); b != 0 {
		t.Fatalf("p0 append1 base=%d want 0", b)
	}
	if b, _ := p0.Append(makeBatch(2, 8)); b != 3 {
		t.Fatalf("p0 append2 base=%d want 3", b)
	}
	if b, _ := p1.Append(makeBatch(2, 8)); b != 0 {
		t.Fatalf("p1 append base=%d want 0", b)
	}
	if p0.LatestOffset() != 5 || p1.LatestOffset() != 2 {
		t.Fatalf("LEOs p0=%d p1=%d want 5,2", p0.LatestOffset(), p1.LatestOffset())
	}

	// Flush => one stream-set object batching both partitions.
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	refs0 := s.index.refsFor("t", 0)
	refs1 := s.index.refsFor("t", 1)
	if len(refs0) != 1 || len(refs1) != 1 {
		t.Fatalf("index refs p0=%d p1=%d want 1,1", len(refs0), len(refs1))
	}
	if refs0[0].Key != refs1[0].Key {
		t.Fatalf("partitions landed in different objects: %q vs %q", refs0[0].Key, refs1[0].Key)
	}
	if refs0[0].BaseOffset != 0 || refs0[0].NextOffset != 5 {
		t.Fatalf("p0 ref range [%d,%d) want [0,5)", refs0[0].BaseOffset, refs0[0].NextOffset)
	}

	// The object's bytes, sliced by the index, must decode back to the right offsets.
	obj, err := store.Get(context.Background(), refs0[0].Key, 0, 0)
	if err != nil {
		t.Fatalf("Get object: %v", err)
	}
	assertBatches(t, "p0", obj[refs0[0].Position:refs0[0].Position+refs0[0].Length], []int64{0, 3})
	assertBatches(t, "p1", obj[refs1[0].Position:refs1[0].Position+refs1[0].Length], []int64{0})
}

func TestObjectLogWALRecovery(t *testing.T) {
	walDir := t.TempDir()
	idx := NewMemIndexStore() // stands in for the FSM: survives the simulated restart

	s1, _, _ := newTestStorage(t, walDir, idx)
	p0, _ := s1.Log("t", 0)
	p0.Append(makeBatch(3, 8)) // 0,1,2
	if err := s1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Two more records, made durable in the WAL but NOT uploaded (ObjectLog.Flush syncs the
	// WAL only). This is the crash window.
	p0.Append(makeBatch(2, 8)) // 3,4
	if err := p0.Flush(); err != nil {
		t.Fatalf("WAL flush: %v", err)
	}
	// Simulate a crash: abandon s1 (no Close => no upload of the pending tail).

	// Restart against the same WAL dir + index.
	s2, _, _ := newTestStorage(t, walDir, idx)
	defer s2.Close()
	rp0, err := s2.Log("t", 0)
	if err != nil {
		t.Fatalf("reopen Log: %v", err)
	}
	if rp0.LatestOffset() != 5 {
		t.Fatalf("recovered LEO=%d want 5 (3 uploaded + 2 from WAL)", rp0.LatestOffset())
	}
	if rp0.EarliestOffset() != 0 {
		t.Fatalf("recovered earliest=%d want 0", rp0.EarliestOffset())
	}
	// The recovered tail uploads on the next flush, extending the committed range to [0,5).
	if err := s2.Flush(); err != nil {
		t.Fatalf("post-recovery Flush: %v", err)
	}
	if l, ok := s2.index.latest("t", 0); !ok || l != 5 {
		t.Fatalf("post-recovery index latest=%d,%v want 5,true", l, ok)
	}
}

// TestObjectLogReadPath reads back records that span the two sources: some uploaded into an
// object, the rest still in the pending buffer. Both must be served transparently, and the
// out-of-range / caught-up edges must match *storage.Log.
func TestObjectLogReadPath(t *testing.T) {
	idx := NewMemIndexStore()
	s, _, _ := newTestStorage(t, t.TempDir(), idx)
	defer s.Close()
	p0, err := s.Log("t", 0)
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	p0.Append(makeBatch(3, 8)) // [0,3)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	p0.Append(makeBatch(2, 16)) // [3,5) — pending, not uploaded

	// Read from the uploaded object.
	if got, err := p0.Read(0, 1<<20); err != nil || len(got) == 0 {
		t.Fatalf("Read(0)=%d bytes err=%v", len(got), err)
	} else {
		assertBatches(t, "uploaded", got, []int64{0})
	}
	// Read from the pending buffer.
	if got, err := p0.Read(3, 1<<20); err != nil {
		t.Fatalf("Read(3): %v", err)
	} else {
		assertBatches(t, "pending", got, []int64{3})
	}
	// Caught up at the LEO: nil, nil.
	if got, err := p0.Read(5, 1<<20); err != nil || got != nil {
		t.Fatalf("Read(5)=%v,%v want nil,nil", got, err)
	}
	// Past the LEO: out of range.
	if _, err := p0.Read(6, 1<<20); !errors.Is(err, storage.ErrOffsetOutOfRange) {
		t.Fatalf("Read(6) err=%v want ErrOffsetOutOfRange", err)
	}
}

// TestObjectLogReadParity asserts the object backend returns byte-identical Read results to the
// file-based *storage.Log for the same append sequence. It runs two single-source regimes —
// all data uploaded, and all data still pending — so both read code paths are exercised against
// the same authoritative reference. (The mixed case can legitimately return fewer batches per
// call, since the object read boundary is the upload boundary, not the log segment; that path
// is covered functionally by TestObjectLogReadPath.)
func TestObjectLogReadParity(t *testing.T) {
	for _, regime := range []struct {
		name   string
		upload bool
	}{
		{"uploaded", true},
		{"pending", false},
	} {
		t.Run(regime.name, func(t *testing.T) {
			s, _, _ := newTestStorage(t, t.TempDir(), NewMemIndexStore())
			defer s.Close()
			op, err := s.Log("t", 0)
			if err != nil {
				t.Fatalf("object Log: %v", err)
			}
			lp, err := storage.Open(t.TempDir(), storage.DefaultConfig())
			if err != nil {
				t.Fatalf("storage.Open: %v", err)
			}
			defer lp.Close()

			for _, b := range [][]byte{makeBatch(3, 8), makeBatch(2, 16), makeBatch(1, 8)} {
				ob := append([]byte(nil), b...)
				lb := append([]byte(nil), b...)
				if _, err := op.Append(ob); err != nil {
					t.Fatalf("object append: %v", err)
				}
				if _, err := lp.Append(lb); err != nil {
					t.Fatalf("log append: %v", err)
				}
			}
			if regime.upload {
				if err := s.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
			}

			for _, maxBytes := range []int32{1, 1 << 20} {
				for off := int64(0); off <= 6; off++ {
					want, werr := lp.Read(off, maxBytes)
					got, gerr := op.Read(off, maxBytes)
					if (werr == nil) != (gerr == nil) {
						t.Fatalf("off=%d max=%d err mismatch: log=%v object=%v", off, maxBytes, werr, gerr)
					}
					if !bytes.Equal(want, got) {
						t.Fatalf("off=%d max=%d bytes mismatch: log=%d object=%d", off, maxBytes, len(want), len(got))
					}
				}
			}
		})
	}
}

// TestObjectLogOffsetForTimestamp checks the time index across uploaded + pending data.
func TestObjectLogOffsetForTimestamp(t *testing.T) {
	s, _, _ := newTestStorage(t, t.TempDir(), NewMemIndexStore())
	defer s.Close()
	p0, err := s.Log("t", 0)
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	p0.Append(makeTimedBatch(3, 100, 8)) // [0,3), maxTs 100 — uploaded
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	p0.Append(makeTimedBatch(2, 200, 8)) // [3,5), maxTs 200 — pending

	cases := []struct {
		ts    int64
		off   int64
		found bool
	}{
		{50, 0, true},   // first batch (uploaded)
		{150, 3, true},  // pending batch
		{200, 3, true},  // exact match on pending
		{201, 0, false}, // beyond all data
	}
	for _, c := range cases {
		off, ok := p0.OffsetForTimestamp(c.ts)
		if ok != c.found || (ok && off != c.off) {
			t.Fatalf("ts=%d got (%d,%v) want (%d,%v)", c.ts, off, ok, c.off, c.found)
		}
	}
}

// TestObjectLogAppendReplica exercises the follower write path: leader-assigned base offsets are
// preserved, the LEO advances while the HWM is driven externally, duplicates are no-ops, and a
// gap is rejected.
func TestObjectLogAppendReplica(t *testing.T) {
	s, _, _ := newTestStorage(t, t.TempDir(), NewMemIndexStore())
	defer s.Close()
	p0, err := s.Log("t", 0)
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	p0.HoldHighWatermark()

	// Leader-assigned batch based at 0 (3 records).
	if leo, err := p0.AppendReplica(replicaBatch(0, 3)); err != nil || leo != 3 {
		t.Fatalf("AppendReplica leo=%d err=%v want 3,nil", leo, err)
	}
	if p0.LatestOffset() != 3 || p0.HighWatermark() != 0 {
		t.Fatalf("LEO=%d HWM=%d want 3,0 (replica advances LEO only)", p0.LatestOffset(), p0.HighWatermark())
	}
	// Duplicate (base below the LEO) is a harmless no-op.
	if leo, err := p0.AppendReplica(replicaBatch(0, 3)); err != nil || leo != 3 {
		t.Fatalf("dup AppendReplica leo=%d err=%v want 3,nil", leo, err)
	}
	// Gap (base above the LEO) is rejected.
	if _, err := p0.AppendReplica(replicaBatch(5, 2)); err == nil {
		t.Fatal("AppendReplica gap: want error")
	}
	// The next in-order batch (base 3) appends.
	if leo, err := p0.AppendReplica(replicaBatch(3, 2)); err != nil || leo != 5 {
		t.Fatalf("AppendReplica leo=%d err=%v want 5,nil", leo, err)
	}

	// Follower applies the leader-reported HWM; the replicated data reads back.
	p0.SetHighWatermark(5)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got, err := p0.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	assertBatches(t, "replica", got, []int64{0, 3})
}

// TestObjectLogRetentionBySize drops the oldest objects to fit a byte budget, advances the
// earliest offset, and physically deletes the objects no partition references anymore.
func TestObjectLogRetentionBySize(t *testing.T) {
	s, store, nodeID := newTestStorage(t, t.TempDir(), NewMemIndexStore())
	defer s.Close()
	p0, err := s.Log("t", 0)
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	// Three separate objects (flush between appends): [0,2) [2,4) [4,6).
	for i := 0; i < 3; i++ {
		p0.Append(makeBatch(2, 100))
		if err := s.Flush(); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
	}
	if got := len(s.index.refsFor("t", 0)); got != 3 {
		t.Fatalf("refs=%d want 3", got)
	}
	if got := len(objectKeys(t, store, nodeID)); got != 3 {
		t.Fatalf("objects=%d want 3", got)
	}

	// Keep roughly one object's worth: drop the two oldest.
	oneObj := p0.Size() / 3
	if err := p0.EnforceRetention(0, oneObj+1); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if got := len(s.index.refsFor("t", 0)); got != 1 {
		t.Fatalf("refs after retention=%d want 1", got)
	}
	if p0.EarliestOffset() != 4 {
		t.Fatalf("earliest=%d want 4", p0.EarliestOffset())
	}
	// Below the new earliest is out of range; at/after it still reads.
	if _, err := p0.Read(0, 1<<20); !errors.Is(err, storage.ErrOffsetOutOfRange) {
		t.Fatalf("Read(0) err=%v want ErrOffsetOutOfRange", err)
	}
	if got, err := p0.Read(4, 1<<20); err != nil || len(got) == 0 {
		t.Fatalf("Read(4)=%d bytes err=%v", len(got), err)
	}
	// The two dropped objects were collected.
	if got := len(objectKeys(t, store, nodeID)); got != 1 {
		t.Fatalf("objects after retention=%d want 1", got)
	}
}

// TestObjectLogRetentionByAge drops an aged-out uploaded object while never touching the pending
// tail (the active-segment analogue).
func TestObjectLogRetentionByAge(t *testing.T) {
	s, store, nodeID := newTestStorage(t, t.TempDir(), NewMemIndexStore())
	defer s.Close()
	p0, err := s.Log("t", 0)
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	p0.Append(makeBatch(2, 8)) // [0,2)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	p0.Append(makeBatch(2, 8)) // [2,4) — still pending

	// Expire anything older than 10ms: the uploaded object goes, the pending tail stays.
	if err := p0.EnforceRetention(10, 0); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if got := len(s.index.refsFor("t", 0)); got != 0 {
		t.Fatalf("refs=%d want 0 (uploaded object expired)", got)
	}
	if p0.EarliestOffset() != 2 {
		t.Fatalf("earliest=%d want 2 (pending start)", p0.EarliestOffset())
	}
	if got := len(objectKeys(t, store, nodeID)); got != 0 {
		t.Fatalf("objects=%d want 0", got)
	}
	// The pending tail still reads.
	if got, err := p0.Read(2, 1<<20); err != nil || len(got) == 0 {
		t.Fatalf("Read(2)=%d bytes err=%v", len(got), err)
	}
}

// replicaBatch builds a leader-assigned batch based at base with count records (offset preset,
// CRC fixed) — the shape AppendReplica receives from the replication fetcher.
func replicaBatch(base int64, count int32) []byte {
	b := makeBatch(count, 8)
	record.SetBaseOffset(b, base)
	record.RecomputeCRC(b)
	return b
}

// objectKeys lists this node's stream-set objects.
func objectKeys(t *testing.T, store ObjectStore, nodeID string) []string {
	t.Helper()
	keys, err := store.List(context.Background(), "data/sso/"+nodeID+"/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return keys
}

// makeTimedBatch is makeBatch with an explicit max timestamp.
func makeTimedBatch(recordCount int32, maxTs int64, padding int) []byte {
	b := makeBatch(recordCount, padding)
	binary.BigEndian.PutUint64(b[35:], uint64(maxTs)) // MaxTimestamp
	record.RecomputeCRC(b)
	return b
}

func assertBatches(t *testing.T, who string, data []byte, wantBases []int64) {
	t.Helper()
	var got []int64
	if _, err := record.Iterate(data, func(b []byte) error {
		h, err := record.ParseHeader(b)
		if err != nil {
			return err
		}
		got = append(got, h.BaseOffset)
		return nil
	}); err != nil {
		t.Fatalf("%s iterate: %v", who, err)
	}
	if len(got) != len(wantBases) {
		t.Fatalf("%s batch bases=%v want %v", who, got, wantBases)
	}
	for i := range wantBases {
		if got[i] != wantBases[i] {
			t.Fatalf("%s batch bases=%v want %v", who, got, wantBases)
		}
	}
}
