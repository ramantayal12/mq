package protocol

import "mq/internal/kbytes"

// CreatePartitionsTopic asks to grow one topic to Count total partitions. Manual replica
// assignments are parsed (to stay on the wire boundary) but ignored — mq seeds the new
// partitions' placement from the cluster view, like CreateTopics.
type CreatePartitionsTopic struct {
	Name  string
	Count int32
}

// CreatePartitionsRequest grows one or more existing topics.
type CreatePartitionsRequest struct {
	Topics       []CreatePartitionsTopic
	TimeoutMs    int32
	ValidateOnly bool
}

func (req *CreatePartitionsRequest) Decode(r *kbytes.Reader, version int16) {
	n := r.ArrayLen()
	req.Topics = make([]CreatePartitionsTopic, n)
	for i := 0; i < n; i++ {
		t := &req.Topics[i]
		t.Name = r.String()
		t.Count = r.Int32()
		// assignment: nullable array of {broker_ids []int32} — ignored by mq.
		na := r.ArrayLen()
		for a := 0; a < na; a++ {
			nb := r.ArrayLen()
			for b := 0; b < nb; b++ {
				r.Int32()
			}
		}
	}
	req.TimeoutMs = r.Int32()
	req.ValidateOnly = r.Int8() != 0
}

// CreatePartitionsTopicResp is one topic's growth result.
type CreatePartitionsTopicResp struct {
	Name         string
	ErrorCode    int16
	ErrorMessage *string
}

// CreatePartitionsResponse is the reply.
type CreatePartitionsResponse struct {
	ThrottleTime int32
	Topics       []CreatePartitionsTopicResp
}

func (resp *CreatePartitionsResponse) Encode(w *kbytes.Writer, version int16) {
	w.Int32(resp.ThrottleTime)
	w.ArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.String(t.Name)
		w.Int16(t.ErrorCode)
		w.NullableString(t.ErrorMessage)
	}
}
