// Package controller is mq's Raft-backed metadata controller — the component that
// plays the role KRaft plays in Kafka. An elected leader owns the authoritative
// cluster metadata (topics, partition replicas, leader, ISR) as a Raft-replicated
// finite state machine, so the metadata survives the death of any single broker.
//
// Phase 1 (see docs/GAPS_PLAN.md) stands the controller up in isolation: the quorum
// forms, a leader is elected, and metadata survives controller failover. Client-visible
// Metadata is still served from the static internal/cluster placement until Phase 2
// routes it through the FSM, so Phase 1 changes no client-visible behavior.
package controller

import (
	"encoding/json"

	"mq/internal/storage/object"
)

// CmdType discriminates the flat Command struct. Values start at 1 so a zero-valued
// Command (e.g. a malformed frame) is never mistaken for a valid mutation.
type CmdType uint8

const (
	CmdCreateTopic      CmdType = iota + 1 // {Topic, Partitions, Replicas}
	CmdCreatePartitions                    // {Topic, Partitions, Replicas} — extend an existing topic
	CmdChangeLeader                        // {Topic, Partition, Leader, ISR} — bumps LeaderEpoch
	CmdChangeISR                           // {Topic, Partition, ISR}
	CmdHeartbeat                           // {From} — liveness ping to the leader; never enters the raft log
	CmdCommitSegment                       // {Topic, Partition, Segment} — record an uploaded object's slice (object backend)
	CmdPruneSegments                       // {Topic, Partition, Cutoff} — drop a partition's refs fully below an offset
)

// Command is the single, flat mutation applied to the FSM. One struct (rather than a
// type per command) keeps JSON encoding trivial; the Type field selects which fields
// are meaningful. Metadata volume is tiny, so JSON's readability in the raft log and
// snapshots is worth more than a compact encoding.
type Command struct {
	Type       CmdType   `json:"t"`
	Topic      string    `json:"topic,omitempty"`
	Partition  int32     `json:"p,omitempty"`
	Partitions int32     `json:"np,omitempty"`       // CreateTopic / CreatePartitions
	Replicas   [][]int32 `json:"replicas,omitempty"` // per-partition replica sets (CreateTopic / CreatePartitions)
	Leader     int32     `json:"leader,omitempty"`
	ISR        []int32   `json:"isr,omitempty"`
	From       int32     `json:"from,omitempty"` // CmdHeartbeat: the heartbeating node (RPC only, never applied)

	Segment *object.SegmentRef `json:"seg,omitempty"`    // CmdCommitSegment: the uploaded object's slice
	Cutoff  int64              `json:"cutoff,omitempty"` // CmdPruneSegments: drop refs with NextOffset <= Cutoff
}

func (c Command) encode() ([]byte, error) { return json.Marshal(c) }

func decodeCommand(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}
