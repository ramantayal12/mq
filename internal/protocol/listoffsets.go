package protocol

import "mq/internal/kbytes"

// Special ListOffsets timestamps.
const (
	TimestampLatest   int64 = -1
	TimestampEarliest int64 = -2
)

// ListOffsetsPartition is one partition's offset query.
type ListOffsetsPartition struct {
	Index         int32
	Timestamp     int64
	MaxNumOffsets int32 // v0 only
}

// ListOffsetsTopic groups partition queries for one topic.
type ListOffsetsTopic struct {
	Name       string
	Partitions []ListOffsetsPartition
}

// ListOffsetsRequest resolves offsets by timestamp (earliest/latest).
type ListOffsetsRequest struct {
	ReplicaID      int32
	IsolationLevel int8
	Topics         []ListOffsetsTopic
}

// Decode reads the request body.
func (req *ListOffsetsRequest) Decode(r *kbytes.Reader, version int16) {
	req.ReplicaID = r.Int32()
	if version >= 2 {
		req.IsolationLevel = r.Int8()
	}
	nt := r.ArrayLen()
	req.Topics = make([]ListOffsetsTopic, nt)
	for i := 0; i < nt; i++ {
		t := &req.Topics[i]
		t.Name = r.String()
		np := r.ArrayLen()
		t.Partitions = make([]ListOffsetsPartition, np)
		for j := 0; j < np; j++ {
			p := &t.Partitions[j]
			p.Index = r.Int32()
			if version >= 4 {
				r.Int32() // current_leader_epoch
			}
			p.Timestamp = r.Int64()
			if version == 0 {
				p.MaxNumOffsets = r.Int32()
			}
		}
	}
}

// ListOffsetsPartitionResp is one partition's result.
type ListOffsetsPartitionResp struct {
	Index     int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

// ListOffsetsTopicResp groups partition results for a topic.
type ListOffsetsTopicResp struct {
	Name       string
	Partitions []ListOffsetsPartitionResp
}

// ListOffsetsResponse is the offset reply.
type ListOffsetsResponse struct {
	ThrottleTime int32
	Topics       []ListOffsetsTopicResp
}

// Encode writes the response body. v0 used an offsets array; v1+ returns a single
// timestamp+offset pair.
func (resp *ListOffsetsResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 2 {
		w.Int32(resp.ThrottleTime)
	}
	w.ArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			if version == 0 {
				// old_style_offsets: a single-element array
				w.ArrayLen(1)
				w.Int64(p.Offset)
			} else {
				w.Int64(p.Timestamp)
				w.Int64(p.Offset)
				if version >= 4 {
					w.Int32(-1) // leader_epoch
				}
			}
		}
	}
}
