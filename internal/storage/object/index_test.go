package object

import "testing"

func TestIndexCommitLoadQuery(t *testing.T) {
	store := NewMemIndexStore()
	ix := newIndex(store)

	ix.commit("t", 0, SegmentRef{Key: "obj/1", BaseOffset: 0, NextOffset: 5, Position: 0, Length: 100})
	ix.commit("t", 0, SegmentRef{Key: "obj/2", BaseOffset: 5, NextOffset: 12, Position: 40, Length: 80})

	if got := ix.refsFor("t", 0); len(got) != 2 {
		t.Fatalf("refsFor = %d refs, want 2", len(got))
	}
	if e, ok := ix.earliest("t", 0); !ok || e != 0 {
		t.Fatalf("earliest = %d,%v want 0,true", e, ok)
	}
	if l, ok := ix.latest("t", 0); !ok || l != 12 {
		t.Fatalf("latest = %d,%v want 12,true", l, ok)
	}

	// A fresh Index over the same store recovers the committed refs (the restart path).
	ix2 := newIndex(store)
	if err := ix2.load("t", 0); err != nil {
		t.Fatalf("load: %v", err)
	}
	if l, ok := ix2.latest("t", 0); !ok || l != 12 {
		t.Fatalf("reloaded latest = %d,%v want 12,true", l, ok)
	}
	if _, ok := ix2.earliest("t", 1); ok {
		t.Fatalf("earliest of unknown partition should be false")
	}
}
