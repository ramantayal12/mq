package protocol

import "mq/internal/kbytes"

// CreateTopicsTopic describes a topic to create. Replica assignments and configs are
// parsed (to stay on the wire boundary) but ignored by the single-broker mq.
type CreateTopicsTopic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

// CreateTopicsRequest creates one or more topics.
type CreateTopicsRequest struct {
	Topics       []CreateTopicsTopic
	TimeoutMs    int32
	ValidateOnly bool
}

func (req *CreateTopicsRequest) Decode(r *kbytes.Reader, version int16) {
	n := r.ArrayLen()
	req.Topics = make([]CreateTopicsTopic, n)
	for i := 0; i < n; i++ {
		t := &req.Topics[i]
		t.Name = r.String()
		t.NumPartitions = r.Int32()
		t.ReplicationFactor = r.Int16()
		// assignments: array of {partition_index int32, broker_ids []int32}
		na := r.ArrayLen()
		for a := 0; a < na; a++ {
			r.Int32()
			nb := r.ArrayLen()
			for b := 0; b < nb; b++ {
				r.Int32()
			}
		}
		// configs: array of {name string, value nullable string} — ignored by mq.
		nc := r.ArrayLen()
		for c := 0; c < nc; c++ {
			_ = r.String()
			_ = r.NullableString()
		}
	}
	req.TimeoutMs = r.Int32()
	if version >= 1 {
		req.ValidateOnly = r.Int8() != 0
	}
}

// CreateTopicsTopicResp is one topic's creation result.
type CreateTopicsTopicResp struct {
	Name         string
	ErrorCode    int16
	ErrorMessage *string
}

// CreateTopicsResponse is the create reply.
type CreateTopicsResponse struct {
	ThrottleTime int32
	Topics       []CreateTopicsTopicResp
}

func (resp *CreateTopicsResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 2 {
		w.Int32(resp.ThrottleTime)
	}
	w.ArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.String(t.Name)
		w.Int16(t.ErrorCode)
		if version >= 1 {
			w.NullableString(t.ErrorMessage)
		}
	}
}
