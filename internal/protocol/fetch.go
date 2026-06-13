package protocol

import "mq/internal/kbytes"

// FetchPartition is one partition fetch request.
type FetchPartition struct {
	Partition      int32
	FetchOffset    int64
	LogStartOffset int64
	MaxBytes       int32
}

// FetchTopic groups partition fetches for one topic.
type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

// FetchRequest reads records from topic-partitions starting at given offsets.
type FetchRequest struct {
	ReplicaID      int32
	MaxWaitMs      int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	SessionID      int32
	SessionEpoch   int32
	Topics         []FetchTopic
}

// Decode reads the request body. Trailing forgotten-topics (v7+) and rack_id (v11+)
// are not needed by mq and are left unread.
func (req *FetchRequest) Decode(r *kbytes.Reader, version int16) {
	req.ReplicaID = r.Int32()
	req.MaxWaitMs = r.Int32()
	req.MinBytes = r.Int32()
	if version >= 3 {
		req.MaxBytes = r.Int32()
	}
	if version >= 4 {
		req.IsolationLevel = r.Int8()
	}
	if version >= 7 {
		req.SessionID = r.Int32()
		req.SessionEpoch = r.Int32()
	}
	nt := r.ArrayLen()
	req.Topics = make([]FetchTopic, nt)
	for i := 0; i < nt; i++ {
		t := &req.Topics[i]
		t.Name = r.String()
		np := r.ArrayLen()
		t.Partitions = make([]FetchPartition, np)
		for j := 0; j < np; j++ {
			p := &t.Partitions[j]
			p.Partition = r.Int32()
			if version >= 9 {
				r.Int32() // current_leader_epoch (unreachable at our cap, kept for safety)
			}
			p.FetchOffset = r.Int64()
			if version >= 5 {
				p.LogStartOffset = r.Int64()
			}
			p.MaxBytes = r.Int32()
		}
	}
}

// FetchPartitionResp is one partition's fetch result.
type FetchPartitionResp struct {
	Partition      int32
	ErrorCode      int16
	HighWatermark  int64
	LastStable     int64
	LogStartOffset int64
	Records        []byte // record set (opaque), nil if none
}

// FetchTopicResp groups partition results for a topic.
type FetchTopicResp struct {
	Name       string
	Partitions []FetchPartitionResp
}

// FetchResponse is the fetch reply.
type FetchResponse struct {
	ThrottleTime int32
	ErrorCode    int16
	SessionID    int32
	Topics       []FetchTopicResp
}

// Encode writes the response body honoring per-version field deltas.
func (resp *FetchResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
	if version >= 7 {
		w.Int16(resp.ErrorCode)
		w.Int32(resp.SessionID)
	}
	w.ArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Partition)
			w.Int16(p.ErrorCode)
			w.Int64(p.HighWatermark)
			if version >= 4 {
				w.Int64(p.LastStable)
			}
			if version >= 5 {
				w.Int64(p.LogStartOffset)
			}
			if version >= 4 {
				w.ArrayLen(0) // aborted_transactions (none)
			}
			if version >= 11 {
				w.Int32(-1) // preferred_read_replica
			}
			w.Bytes(p.Records)
		}
	}
}
