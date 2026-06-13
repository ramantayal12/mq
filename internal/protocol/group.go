package protocol

import "mq/internal/kbytes"

// --- FindCoordinator ---

// FindCoordinatorRequest looks up the coordinator for a group (key = group id).
type FindCoordinatorRequest struct {
	Key     string
	KeyType int8
}

func (req *FindCoordinatorRequest) Decode(r *kbytes.Reader, version int16) {
	req.Key = r.String()
	if version >= 1 {
		req.KeyType = r.Int8()
	}
}

// FindCoordinatorResponse returns the coordinator broker.
type FindCoordinatorResponse struct {
	ThrottleTime int32
	ErrorCode    int16
	ErrorMessage *string
	NodeID       int32
	Host         string
	Port         int32
}

func (resp *FindCoordinatorResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
	w.Int16(resp.ErrorCode)
	if version >= 1 {
		w.NullableString(resp.ErrorMessage)
	}
	w.Int32(resp.NodeID)
	w.String(resp.Host)
	w.Int32(resp.Port)
}

// --- JoinGroup ---

// JoinGroupProtocol is one candidate assignment protocol the member supports.
type JoinGroupProtocol struct {
	Name     string
	Metadata []byte
}

// JoinGroupRequest requests group membership.
type JoinGroupRequest struct {
	GroupID          string
	SessionTimeout   int32
	RebalanceTimeout int32
	MemberID         string
	ProtocolType     string
	Protocols        []JoinGroupProtocol
}

func (req *JoinGroupRequest) Decode(r *kbytes.Reader, version int16) {
	req.GroupID = r.String()
	req.SessionTimeout = r.Int32()
	if version >= 1 {
		req.RebalanceTimeout = r.Int32()
	}
	req.MemberID = r.String()
	req.ProtocolType = r.String()
	n := r.ArrayLen()
	req.Protocols = make([]JoinGroupProtocol, n)
	for i := 0; i < n; i++ {
		req.Protocols[i].Name = r.String()
		req.Protocols[i].Metadata = r.Bytes()
	}
}

// JoinGroupMember is a member echoed to the elected leader.
type JoinGroupMember struct {
	MemberID string
	Metadata []byte
}

// JoinGroupResponse is the join result.
type JoinGroupResponse struct {
	ThrottleTime int32
	ErrorCode    int16
	GenerationID int32
	ProtocolName string
	Leader       string
	MemberID     string
	Members      []JoinGroupMember
}

func (resp *JoinGroupResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 2 {
		w.Int32(resp.ThrottleTime)
	}
	w.Int16(resp.ErrorCode)
	w.Int32(resp.GenerationID)
	w.String(resp.ProtocolName)
	w.String(resp.Leader)
	w.String(resp.MemberID)
	w.ArrayLen(len(resp.Members))
	for _, m := range resp.Members {
		w.String(m.MemberID)
		w.Bytes(m.Metadata)
	}
}

// --- SyncGroup (capped at v2: no group_instance_id) ---

// SyncGroupAssignment maps a member to its assignment bytes.
type SyncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

// SyncGroupRequest distributes the leader's assignment.
type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []SyncGroupAssignment
}

func (req *SyncGroupRequest) Decode(r *kbytes.Reader, version int16) {
	req.GroupID = r.String()
	req.GenerationID = r.Int32()
	req.MemberID = r.String()
	n := r.ArrayLen()
	req.Assignments = make([]SyncGroupAssignment, n)
	for i := 0; i < n; i++ {
		req.Assignments[i].MemberID = r.String()
		req.Assignments[i].Assignment = r.Bytes()
	}
}

// SyncGroupResponse returns this member's assignment.
type SyncGroupResponse struct {
	ThrottleTime int32
	ErrorCode    int16
	Assignment   []byte
}

func (resp *SyncGroupResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
	w.Int16(resp.ErrorCode)
	w.Bytes(resp.Assignment)
}

// --- Heartbeat (capped at v2) ---

// HeartbeatRequest keeps a member alive within its group/generation.
type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

func (req *HeartbeatRequest) Decode(r *kbytes.Reader, version int16) {
	req.GroupID = r.String()
	req.GenerationID = r.Int32()
	req.MemberID = r.String()
}

// HeartbeatResponse is the heartbeat result.
type HeartbeatResponse struct {
	ThrottleTime int32
	ErrorCode    int16
}

func (resp *HeartbeatResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
	w.Int16(resp.ErrorCode)
}

// --- LeaveGroup (capped at v2: single member, no batch) ---

// LeaveGroupRequest removes a member from a group.
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

func (req *LeaveGroupRequest) Decode(r *kbytes.Reader, version int16) {
	req.GroupID = r.String()
	req.MemberID = r.String()
}

// LeaveGroupResponse is the leave result.
type LeaveGroupResponse struct {
	ThrottleTime int32
	ErrorCode    int16
}

func (resp *LeaveGroupResponse) Encode(w *kbytes.Writer, version int16) {
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
	w.Int16(resp.ErrorCode)
}
