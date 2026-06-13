package group

import (
	"testing"
)

func newTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	store, err := NewOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := NewCoordinator(store)
	t.Cleanup(c.Close)
	return c
}

func coopJoin(c *Coordinator, group, memberID string) JoinResult {
	return c.Join(JoinArgs{
		GroupID:          group,
		MemberID:         memberID,
		ClientID:         "client",
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "cooperative-sticky", Metadata: []byte{0x1}}},
		SessionTimeoutMs: 30000,
	})
}

// TestRejoinOnStableGroupOpensRebalance pins Kafka's semantics that *any* JoinGroup —
// including a rejoin by an existing member of a Stable group — opens a new rebalance
// generation. franz-go's cooperative-sticky assignor relies on this: it revokes a moved
// partition in one generation and re-sends JoinGroup to get it reassigned in the next.
// If a stable-group rejoin is ignored, that follow-up never happens and the revoked
// partition is orphaned (the missing=N rebalance loss).
func TestRejoinOnStableGroupOpensRebalance(t *testing.T) {
	c := newTestCoordinator(t)
	const g = "grp"

	// One member joins and the group reaches Stable (leader syncs its assignment).
	j1 := coopJoin(c, g, "")
	if sr := c.Sync(g, j1.MemberID, j1.GenerationID, map[string][]byte{j1.MemberID: {0x1}}); sr.ErrorCode != ErrNone {
		t.Fatalf("leader sync failed: code %d", sr.ErrorCode)
	}
	if d, _ := c.DescribeGroup(g); d.State != "Stable" {
		t.Fatalf("group state = %q, want Stable", d.State)
	}

	// The same member rejoins (the cooperative follow-up). This must open a new
	// generation so the leader is prompted to reassign.
	j2 := coopJoin(c, g, j1.MemberID)
	if j2.GenerationID <= j1.GenerationID {
		t.Fatalf("rejoin on a Stable group did not open a new generation: was %d, still %d", j1.GenerationID, j2.GenerationID)
	}
	if d, _ := c.DescribeGroup(g); d.State != "CompletingRebalance" {
		t.Fatalf("group state after rejoin = %q, want CompletingRebalance", d.State)
	}

	// And the group can re-converge: the member syncs at the new generation back to Stable.
	if sr := c.Sync(g, j2.MemberID, j2.GenerationID, map[string][]byte{j2.MemberID: {0x1}}); sr.ErrorCode != ErrNone {
		t.Fatalf("re-sync failed: code %d", sr.ErrorCode)
	}
	if d, _ := c.DescribeGroup(g); d.State != "Stable" {
		t.Fatalf("group did not return to Stable after re-sync, got %q", d.State)
	}
}
