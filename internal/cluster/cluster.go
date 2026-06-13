// Package cluster models static broker membership and deterministic partition
// placement. It is mq's stand-in for Kafka's controller quorum (KRaft/ZooKeeper):
// instead of electing leadership through consensus, every broker is configured with
// the same ordered member list and computes the same placement as a pure function,
// so all brokers independently agree on which broker leads each partition and which
// broker coordinates each consumer group.
//
// This mirrors how a real Kafka cluster behaves from a client's point of view —
// Metadata advertises all brokers and a leader per partition, FindCoordinator points
// at one coordinator per group — with replication factor 1 (one copy per partition).
package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

// BrokerInfo is one member's identity and address.
type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
}

// Cluster is the static membership view held by a single broker.
type Cluster struct {
	self    int32
	brokers []BrokerInfo // sorted by NodeID (stable placement across brokers)
}

// Single builds a one-node cluster (the default, backward-compatible mode).
func Single(nodeID int32, host string, port int32) *Cluster {
	return &Cluster{self: nodeID, brokers: []BrokerInfo{{NodeID: nodeID, Host: host, Port: port}}}
}

// Parse builds a Cluster from a member spec "id@host:port,id@host:port,...".
// When spec is empty, a single-node cluster is returned. selfHost/selfPort are used
// only in the single-node case.
func Parse(nodeID int32, spec, selfHost string, selfPort int32) (*Cluster, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return Single(nodeID, selfHost, selfPort), nil
	}
	var brokers []BrokerInfo
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		at := strings.Index(part, "@")
		if at < 0 {
			return nil, fmt.Errorf("cluster: bad broker spec %q (want id@host:port)", part)
		}
		id, err := strconv.Atoi(part[:at])
		if err != nil {
			return nil, fmt.Errorf("cluster: bad node id in %q: %w", part, err)
		}
		hostPort := part[at+1:]
		colon := strings.LastIndex(hostPort, ":")
		if colon < 0 {
			return nil, fmt.Errorf("cluster: missing port in %q", part)
		}
		port, err := strconv.Atoi(hostPort[colon+1:])
		if err != nil {
			return nil, fmt.Errorf("cluster: bad port in %q: %w", part, err)
		}
		brokers = append(brokers, BrokerInfo{NodeID: int32(id), Host: hostPort[:colon], Port: int32(port)})
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("cluster: no brokers parsed from %q", spec)
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].NodeID < brokers[j].NodeID })
	c := &Cluster{self: nodeID, brokers: brokers}
	if _, ok := c.Broker(nodeID); !ok {
		return nil, fmt.Errorf("cluster: self node id %d not present in member list", nodeID)
	}
	return c, nil
}

// Self returns this broker's node id.
func (c *Cluster) Self() int32 { return c.self }

// Size returns the number of brokers.
func (c *Cluster) Size() int { return len(c.brokers) }

// Brokers returns the membership (sorted by node id).
func (c *Cluster) Brokers() []BrokerInfo { return c.brokers }

// Broker returns the member with the given node id.
func (c *Cluster) Broker(id int32) (BrokerInfo, bool) {
	for _, b := range c.brokers {
		if b.NodeID == id {
			return b, true
		}
	}
	return BrokerInfo{}, false
}

// LeaderFor returns the node id that leads (topic, partition). Placement is a pure
// function of the topic name, partition index and member list, so every broker
// computes the same answer without coordination. (Replication factor 1: the leader
// is the only replica.)
func (c *Cluster) LeaderFor(topic string, partition int32) int32 {
	idx := (hash32(topic) + uint32(partition)) % uint32(len(c.brokers))
	return c.brokers[idx].NodeID
}

// IsLeader reports whether this broker leads (topic, partition).
func (c *Cluster) IsLeader(topic string, partition int32) bool {
	return c.LeaderFor(topic, partition) == c.self
}

// GroupCoordinator returns the node id that coordinates the given consumer group.
// Kafka hashes the group id to an internal __consumer_offsets partition and uses
// that partition's leader; mq collapses that to a direct hash → broker, giving the
// same property: exactly one broker owns each group.
func (c *Cluster) GroupCoordinator(groupID string) int32 {
	idx := hash32(groupID) % uint32(len(c.brokers))
	return c.brokers[idx].NodeID
}

// IsCoordinator reports whether this broker coordinates the given group.
func (c *Cluster) IsCoordinator(groupID string) bool {
	return c.GroupCoordinator(groupID) == c.self
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
