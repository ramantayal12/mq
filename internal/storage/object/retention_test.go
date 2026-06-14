package object

// Unit tests for the retention index mechanics — pure index operations, no MinIO.

import "testing"

func TestIndexPruneAndReferenced(t *testing.T) {
	ix := newIndex(NewMemIndexStore())
	// Object "a" batches p0 [0,5) and p1 [0,3); object "b" holds only p0 [5,8).
	ix.commit("t", 0, SegmentRef{Key: "a", BaseOffset: 0, NextOffset: 5, Length: 100})
	ix.commit("t", 0, SegmentRef{Key: "b", BaseOffset: 5, NextOffset: 8, Length: 60})
	ix.commit("t", 1, SegmentRef{Key: "a", BaseOffset: 0, NextOffset: 3, Length: 40})

	// Prune p0 below offset 5: drops p0's "a" slice, keeps "b".
	dropped, err := ix.prune("t", 0, 5)
	if err != nil {
		t.Fatalf("prune p0: %v", err)
	}
	if len(dropped) != 1 || dropped[0].Key != "a" {
		t.Fatalf("dropped=%v want one ref keyed a", dropped)
	}
	if refs := ix.refsFor("t", 0); len(refs) != 1 || refs[0].Key != "b" {
		t.Fatalf("p0 remaining=%v want [b]", refs)
	}
	// "a" is still referenced by p1, so it is not collectible yet.
	if !ix.referenced("a") {
		t.Fatal("a should still be referenced by p1")
	}

	// Prune p1 too: now nothing references "a".
	if _, err := ix.prune("t", 1, 3); err != nil {
		t.Fatalf("prune p1: %v", err)
	}
	if ix.referenced("a") {
		t.Fatal("a should be unreferenced after pruning p1")
	}
	if !ix.referenced("b") {
		t.Fatal("b should still be referenced by p0")
	}
}
