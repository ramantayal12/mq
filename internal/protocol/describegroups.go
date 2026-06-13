package protocol

import "mq/internal/kbytes"

// --- DescribeGroups (capped at v2: no authorized_operations, no group_instance_id) ---

// DescribeGroupsRequest asks for details of the named groups.
type DescribeGroupsRequest struct {
	Groups []string
}

// Decode reads the request body (an array of group ids).
func (req *DescribeGroupsRequest) Decode(r *kbytes.Reader, version int16) {
	n := r.ArrayLen()
	req.Groups = make([]string, n)
	for i := 0; i < n; i++ {
		req.Groups[i] = r.String()
	}
}

// DescribeGroupsMember describes one member of a group.
type DescribeGroupsMember struct {
	MemberID   string
	ClientID   string
	ClientHost string
	Metadata   []byte
	Assignment []byte
}

// DescribeGroupsGroup is one group's full description.
type DescribeGroupsGroup struct {
	ErrorCode    int16
	GroupID      string
	State        string
	ProtocolType string
	Protocol     string // the chosen protocol name (protocol_data on the wire)
	Members      []DescribeGroupsMember
}

// DescribeGroupsResponse describes the requested groups.
type DescribeGroupsResponse struct {
	ThrottleTime int32
	Groups       []DescribeGroupsGroup
}

// Encode writes the response body. v1+ prefixes a throttle time.
func (resp *DescribeGroupsResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
	w.ArrayLen(len(resp.Groups))
	for _, g := range resp.Groups {
		w.Int16(g.ErrorCode)
		w.String(g.GroupID)
		w.String(g.State)
		w.String(g.ProtocolType)
		w.String(g.Protocol)
		w.ArrayLen(len(g.Members))
		for _, m := range g.Members {
			w.String(m.MemberID)
			w.String(m.ClientID)
			w.String(m.ClientHost)
			w.Bytes(m.Metadata)
			w.Bytes(m.Assignment)
		}
	}
}
