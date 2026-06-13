package protocol

import "mq/internal/kbytes"

// --- ListGroups (capped at v2: no state filter, no per-group state field) ---

// ListGroupsRequest lists consumer groups. v0-v2 carry no request fields.
type ListGroupsRequest struct{}

// Decode reads the (empty) request body.
func (req *ListGroupsRequest) Decode(r *kbytes.Reader, version int16) {}

// ListGroupsGroup is one group in the listing.
type ListGroupsGroup struct {
	GroupID      string
	ProtocolType string
}

// ListGroupsResponse lists the groups this broker coordinates.
type ListGroupsResponse struct {
	ThrottleTime int32
	ErrorCode    int16
	Groups       []ListGroupsGroup
}

// Encode writes the response body. v1+ prefixes a throttle time.
func (resp *ListGroupsResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
	w.Int16(resp.ErrorCode)
	w.ArrayLen(len(resp.Groups))
	for _, g := range resp.Groups {
		w.String(g.GroupID)
		w.String(g.ProtocolType)
	}
}
