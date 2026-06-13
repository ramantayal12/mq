package protocol

import "mq/internal/kbytes"

// MetadataRequest carries the requested topic names. An empty/null list means "all
// topics". The auth-related trailing flags (v4+/v8+) are not needed by mq and are
// left unread.
type MetadataRequest struct {
	Topics    []string
	AllTopics bool
}

// Decode reads the request body.
func (req *MetadataRequest) Decode(r *kbytes.Reader, version int16) {
	n := r.ArrayLen()
	if n == 0 {
		req.AllTopics = true
		return
	}
	req.Topics = make([]string, n)
	for i := 0; i < n; i++ {
		req.Topics[i] = r.String()
	}
}

// MetadataBroker describes one broker node.
type MetadataBroker struct {
	NodeID int32
	Host   string
	Port   int32
}

// MetadataPartition describes one partition's placement.
type MetadataPartition struct {
	ErrorCode int16
	Index     int32
	Leader    int32
	Replicas  []int32
	Isr       []int32
}

// MetadataTopic describes a topic and its partitions.
type MetadataTopic struct {
	ErrorCode  int16
	Name       string
	IsInternal bool
	Partitions []MetadataPartition
}

// MetadataResponse is the full metadata reply.
type MetadataResponse struct {
	ThrottleTime int32
	Brokers      []MetadataBroker
	ClusterID    *string
	ControllerID int32
	Topics       []MetadataTopic
}

// Encode writes the response body honoring per-version field deltas.
func (resp *MetadataResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 3 {
		w.Int32(resp.ThrottleTime)
	}
	w.ArrayLen(len(resp.Brokers))
	for _, b := range resp.Brokers {
		w.Int32(b.NodeID)
		w.String(b.Host)
		w.Int32(b.Port)
		if version >= 1 {
			w.NullableString(nil) // rack
		}
	}
	if version >= 2 {
		w.NullableString(resp.ClusterID)
	}
	if version >= 1 {
		w.Int32(resp.ControllerID)
	}
	w.ArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.Int16(t.ErrorCode)
		w.String(t.Name)
		if version >= 1 {
			w.Bool(t.IsInternal)
		}
		w.ArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int16(p.ErrorCode)
			w.Int32(p.Index)
			w.Int32(p.Leader)
			if version >= 7 {
				w.Int32(0) // leader_epoch
			}
			w.ArrayLen(len(p.Replicas))
			for _, r := range p.Replicas {
				w.Int32(r)
			}
			w.ArrayLen(len(p.Isr))
			for _, r := range p.Isr {
				w.Int32(r)
			}
			if version >= 5 {
				w.ArrayLen(0) // offline_replicas
			}
		}
		if version >= 8 {
			w.Int32(-1) // topic_authorized_operations
		}
	}
	if version >= 8 {
		w.Int32(-1) // cluster_authorized_operations
	}
}
