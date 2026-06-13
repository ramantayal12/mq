package protocol

import "mq/internal/kbytes"

// ProducePartition is one partition's record set within a Produce request.
type ProducePartition struct {
	Index   int32
	Records []byte // opaque record set (one or more v2 batches)
}

// ProduceTopic groups partitions for one topic.
type ProduceTopic struct {
	Name       string
	Partitions []ProducePartition
}

// ProduceRequest is a produce of record sets to topic-partitions.
type ProduceRequest struct {
	TransactionalID *string
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceTopic
}

// Decode reads the request body.
func (req *ProduceRequest) Decode(r *kbytes.Reader, version int16) {
	if version >= 3 {
		req.TransactionalID = r.NullableString()
	}
	req.Acks = r.Int16()
	req.TimeoutMs = r.Int32()
	nt := r.ArrayLen()
	req.Topics = make([]ProduceTopic, nt)
	for i := 0; i < nt; i++ {
		t := &req.Topics[i]
		t.Name = r.String()
		np := r.ArrayLen()
		t.Partitions = make([]ProducePartition, np)
		for j := 0; j < np; j++ {
			t.Partitions[j].Index = r.Int32()
			t.Partitions[j].Records = r.Bytes()
		}
	}
}

// ProducePartitionResp is one partition's produce result.
type ProducePartitionResp struct {
	Index          int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
}

// ProduceTopicResp groups partition results for a topic.
type ProduceTopicResp struct {
	Name       string
	Partitions []ProducePartitionResp
}

// ProduceResponse is the produce reply.
type ProduceResponse struct {
	Topics       []ProduceTopicResp
	ThrottleTime int32
}

// Encode writes the response body honoring per-version field deltas.
func (resp *ProduceResponse) Encode(w *kbytes.Writer, version int16) {
	w.ArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.Int64(p.BaseOffset)
			if version >= 2 {
				w.Int64(p.LogAppendTime)
			}
			if version >= 5 {
				w.Int64(p.LogStartOffset)
			}
		}
	}
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
}
