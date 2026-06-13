package cluster

import "testing"

func TestParseAndPlacementDeterminism(t *testing.T) {
	spec := "0@h0:9092,1@h1:9092,2@h2:9092"
	// Three brokers, each parsing the same spec, must agree on every placement.
	c0, err := Parse(0, spec, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	c1, _ := Parse(1, spec, "", 0)
	c2, _ := Parse(2, spec, "", 0)

	if c0.Size() != 3 {
		t.Fatalf("size=%d", c0.Size())
	}
	for _, topic := range []string{"events", "orders", "a", "b", "c"} {
		for p := int32(0); p < 6; p++ {
			l0 := c0.LeaderFor(topic, p)
			if l0 != c1.LeaderFor(topic, p) || l0 != c2.LeaderFor(topic, p) {
				t.Fatalf("leader disagreement for %s-%d", topic, p)
			}
		}
		g0 := c0.GroupCoordinator(topic)
		if g0 != c1.GroupCoordinator(topic) || g0 != c2.GroupCoordinator(topic) {
			t.Fatalf("coordinator disagreement for %s", topic)
		}
	}

	// Exactly one broker leads any given partition.
	count := 0
	for _, c := range []*Cluster{c0, c1, c2} {
		if c.IsLeader("events", 0) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", count)
	}
}

func TestSingleNodeDefault(t *testing.T) {
	c, err := Parse(0, "", "localhost", 9092)
	if err != nil {
		t.Fatal(err)
	}
	if c.Size() != 1 || !c.IsLeader("anything", 5) || !c.IsCoordinator("g1") {
		t.Fatal("single-node cluster should lead/coordinate everything")
	}
}

func TestSelfMustBeMember(t *testing.T) {
	if _, err := Parse(9, "0@h0:9092,1@h1:9092", "", 0); err == nil {
		t.Fatal("expected error when self not in member list")
	}
}
