package object

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendReplay(t *testing.T) {
	w, err := openWAL(t.TempDir())
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	recs := []walRecord{
		{Topic: "a", Partition: 0, BaseOffset: 0, Batch: []byte("batch-a0")},
		{Topic: "b", Partition: 2, BaseOffset: 0, Batch: []byte("batch-b0")},
		{Topic: "a", Partition: 0, BaseOffset: 1, Batch: []byte("batch-a1")},
	}
	for _, r := range recs {
		if err := w.append(r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	var got []walRecord
	if err := w.replay(func(r walRecord) error { got = append(got, r); return nil }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != len(recs) {
		t.Fatalf("replay got %d records, want %d", len(got), len(recs))
	}
	for i := range recs {
		if got[i].Topic != recs[i].Topic || got[i].Partition != recs[i].Partition ||
			got[i].BaseOffset != recs[i].BaseOffset || string(got[i].Batch) != string(recs[i].Batch) {
			t.Fatalf("record %d = %+v, want %+v", i, got[i], recs[i])
		}
	}
}

func TestWALRotateAndTrim(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	if err := w.append(walRecord{Topic: "a", Batch: []byte("old")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	sealed, err := w.rotate()
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := w.append(walRecord{Topic: "a", BaseOffset: 1, Batch: []byte("new")}); err != nil {
		t.Fatalf("append after rotate: %v", err)
	}
	// Trim the sealed segment: only the post-rotate record should survive.
	if err := w.deleteThrough(sealed); err != nil {
		t.Fatalf("deleteThrough: %v", err)
	}
	var got []string
	if err := w.replay(func(r walRecord) error { got = append(got, string(r.Batch)); return nil }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 1 || got[0] != "new" {
		t.Fatalf("after trim replay = %v, want [new]", got)
	}
}

func TestWALTornTailRecovers(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	if err := w.append(walRecord{Topic: "a", Batch: []byte("intact")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	seg := w.segPath(w.activeSeq)
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Simulate a crash mid-write: append a few garbage bytes (a torn frame) to the segment.
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	if _, err := f.Write([]byte{walMagic, 0x00, 0x01}); err != nil {
		t.Fatalf("write torn: %v", err)
	}
	f.Close()

	w2, err := openWAL(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	var got []string
	if err := w2.replay(func(r walRecord) error { got = append(got, string(r.Batch)); return nil }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 1 || got[0] != "intact" {
		t.Fatalf("torn-tail replay = %v, want [intact]", got)
	}
	// A reopen must not append to a recovered segment; it opens a fresh one.
	if filepath.Base(seg) == filepath.Base(w2.segPath(w2.activeSeq)) {
		t.Fatalf("reopen reused sealed segment %s", seg)
	}
}
