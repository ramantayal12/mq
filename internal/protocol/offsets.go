package protocol

import "mq/internal/kbytes"

// --- OffsetCommit (capped at v6: no group_instance_id) ---

// OffsetCommitPartition is one partition's committed position.
type OffsetCommitPartition struct {
	Index       int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
}

// OffsetCommitTopic groups partitions for one topic.
type OffsetCommitTopic struct {
	Name       string
	Partitions []OffsetCommitPartition
}

// OffsetCommitRequest persists consumer offsets for a group.
type OffsetCommitRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Topics       []OffsetCommitTopic
}

func (req *OffsetCommitRequest) Decode(r *kbytes.Reader, version int16) {
	req.GroupID = r.String()
	if version >= 1 {
		req.GenerationID = r.Int32()
		req.MemberID = r.String()
	}
	if version >= 2 && version <= 4 {
		r.Int64() // retention_time_ms (unused)
	}
	nt := r.ArrayLen()
	req.Topics = make([]OffsetCommitTopic, nt)
	for i := 0; i < nt; i++ {
		t := &req.Topics[i]
		t.Name = r.String()
		np := r.ArrayLen()
		t.Partitions = make([]OffsetCommitPartition, np)
		for j := 0; j < np; j++ {
			p := &t.Partitions[j]
			p.Index = r.Int32()
			p.Offset = r.Int64()
			if version == 1 {
				r.Int64() // commit_timestamp (v1 only, unused)
			}
			if version >= 6 {
				p.LeaderEpoch = r.Int32()
			}
			p.Metadata = r.NullableString()
		}
	}
}

// OffsetCommitPartitionResp is one partition's commit result.
type OffsetCommitPartitionResp struct {
	Index     int32
	ErrorCode int16
}

// OffsetCommitTopicResp groups partition results for a topic.
type OffsetCommitTopicResp struct {
	Name       string
	Partitions []OffsetCommitPartitionResp
}

// OffsetCommitResponse is the commit reply.
type OffsetCommitResponse struct {
	ThrottleTime int32
	Topics       []OffsetCommitTopicResp
}

func (resp *OffsetCommitResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 3 {
		w.Int32(resp.ThrottleTime)
	}
	w.ArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
		}
	}
}

// --- OffsetFetch (capped at v6) ---

// OffsetFetchTopic is a topic + the partitions to fetch offsets for.
type OffsetFetchTopic struct {
	Name       string
	Partitions []int32
}

// OffsetFetchRequest reads committed offsets for a group. A null topics list
// (v2+) means "all topics".
type OffsetFetchRequest struct {
	GroupID   string
	AllTopics bool
	Topics    []OffsetFetchTopic
}

func (req *OffsetFetchRequest) Decode(r *kbytes.Reader, version int16) {
	req.GroupID = r.String()
	n := int(r.Int32()) // topics array length; -1 (null) means all topics
	if n < 0 {
		req.AllTopics = true
		return
	}
	if n > len(r.Remaining()) { // guard against a malformed/oversized length
		return
	}
	req.Topics = make([]OffsetFetchTopic, n)
	for i := 0; i < n; i++ {
		req.Topics[i].Name = r.String()
		np := r.ArrayLen()
		req.Topics[i].Partitions = make([]int32, np)
		for j := 0; j < np; j++ {
			req.Topics[i].Partitions[j] = r.Int32()
		}
	}
}

// OffsetFetchPartitionResp is one partition's committed offset.
type OffsetFetchPartitionResp struct {
	Index       int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	ErrorCode   int16
}

// OffsetFetchTopicResp groups partition results for a topic.
type OffsetFetchTopicResp struct {
	Name       string
	Partitions []OffsetFetchPartitionResp
}

// OffsetFetchResponse is the offset-fetch reply.
type OffsetFetchResponse struct {
	ThrottleTime int32
	Topics       []OffsetFetchTopicResp
	ErrorCode    int16
}

func (resp *OffsetFetchResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 3 {
		w.Int32(resp.ThrottleTime)
	}
	w.ArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int64(p.Offset)
			if version >= 5 {
				w.Int32(p.LeaderEpoch)
			}
			w.NullableString(p.Metadata)
			w.Int16(p.ErrorCode)
		}
	}
	if version >= 2 {
		w.Int16(resp.ErrorCode)
	}
}
