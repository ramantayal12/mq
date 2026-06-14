package server

import (
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"mq/internal/broker"
	"mq/internal/group"
	"mq/internal/kbytes"
	"mq/internal/metrics"
	"mq/internal/protocol"
	"mq/internal/record"
	"mq/internal/storage"
)

// defaultFetchBytes caps a partition fetch when the client passes 0.
const defaultFetchBytes int32 = 1 << 20

// clusterID is reported in Metadata responses.
var clusterID = "mq-cluster"

// Handler turns decoded requests into broker/storage/group calls and back into
// response bodies. It is the only layer that imports protocol, broker and group
// together (it is the glue; other packages stay single-responsibility).
type Handler struct {
	broker   *broker.Broker
	coord    *group.Coordinator
	reqCount [32]atomic.Int64 // per-API-key request counter (observability/tests)
}

// Requests returns how many requests of the given API key have been handled.
func (h *Handler) Requests(apiKey int16) int64 {
	if apiKey < 0 || int(apiKey) >= len(h.reqCount) {
		return 0
	}
	return h.reqCount[apiKey].Load()
}

// Dispatch decodes and serves one request, returning the response body (after the
// correlation id) and whether a reply should be written at all.
func (h *Handler) Dispatch(hdr protocol.RequestHeader, r *kbytes.Reader) (body []byte, reply bool) {
	if k := hdr.APIKey; k >= 0 && int(k) < len(h.reqCount) {
		h.reqCount[k].Add(1)
	}
	start := time.Now()
	apiLabel := metrics.APIName(hdr.APIKey)
	metrics.Requests.WithLabelValues(apiLabel).Inc()
	defer func() {
		metrics.RequestLatency.WithLabelValues(apiLabel).Observe(time.Since(start).Seconds())
	}()
	v := hdr.APIVersion
	switch hdr.APIKey {
	case protocol.APIApiVersions:
		return h.apiVersions(v), true
	case protocol.APIMetadata:
		return h.metadata(v, r), true
	case protocol.APIProduce:
		return h.produce(v, r)
	case protocol.APIFetch:
		return h.fetch(v, r), true
	case protocol.APIListOffsets:
		return h.listOffsets(v, r), true
	case protocol.APIFindCoordinator:
		return h.findCoordinator(v, r), true
	case protocol.APIJoinGroup:
		return h.joinGroup(v, r), true
	case protocol.APISyncGroup:
		return h.syncGroup(v, r), true
	case protocol.APIHeartbeat:
		return h.heartbeat(v, r), true
	case protocol.APILeaveGroup:
		return h.leaveGroup(v, r), true
	case protocol.APIListGroups:
		return h.listGroups(v, r), true
	case protocol.APIDescribeGroups:
		return h.describeGroups(v, r), true
	case protocol.APIOffsetCommit:
		return h.offsetCommit(v, r), true
	case protocol.APIOffsetFetch:
		return h.offsetFetch(v, r), true
	case protocol.APICreateTopics:
		return h.createTopics(v, r), true
	case protocol.APICreatePartitions:
		return h.createPartitions(v, r), true
	default:
		slog.Warn("unsupported api key", "key", hdr.APIKey, "version", v)
		return nil, false
	}
}

func encode(fn func(w *kbytes.Writer)) []byte {
	w := kbytes.NewWriter()
	fn(w)
	return w.Finish()
}

func (h *Handler) apiVersions(version int16) []byte {
	resp := protocol.ApiVersionsResponse{Versions: protocol.SupportedVersions}
	// Probes for an unsupported version are answered with a v0 body + error 35 so
	// the client can renegotiate.
	if _, ok := rangeFor(protocol.APIApiVersions, version); !ok {
		resp.ErrorCode = protocol.ErrUnsupportedVersion
		return encode(func(w *kbytes.Writer) { resp.Encode(w, 0) })
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func rangeFor(api, version int16) (protocol.VersionRange, bool) {
	vr, ok := protocol.Supported(api)
	if !ok || version < vr.Min || version > vr.Max {
		return vr, false
	}
	return vr, true
}

func (h *Handler) metadata(version int16, r *kbytes.Reader) []byte {
	var req protocol.MetadataRequest
	req.Decode(r, version)
	cl := h.broker.Cluster()

	resp := protocol.MetadataResponse{
		ClusterID:    &clusterID,
		ControllerID: cl.Brokers()[0].NodeID, // lowest node id acts as the controller id
	}
	for _, bi := range cl.Brokers() {
		resp.Brokers = append(resp.Brokers, protocol.MetadataBroker{NodeID: bi.NodeID, Host: bi.Host, Port: bi.Port})
	}

	// Determine which topics to describe.
	type topicReq struct {
		name string
		np   int32
	}
	var topics []topicReq
	if req.AllTopics {
		for _, t := range h.broker.KnownTopics() {
			topics = append(topics, topicReq{t.Name, t.Partitions})
		}
	} else {
		for _, name := range req.Topics {
			if !h.broker.Knows(name) && !h.broker.AutoCreate() {
				resp.Topics = append(resp.Topics, protocol.MetadataTopic{ErrorCode: protocol.ErrUnknownTopicOrPart, Name: name})
				continue
			}
			// Placement is a pure function, so this broker can describe any topic
			// (its partition count defaults cluster-wide) even if another broker
			// leads the partitions. Register it locally so we lead+store our share.
			if h.broker.AutoCreate() {
				h.broker.EnsureTopic(name)
			}
			topics = append(topics, topicReq{name, h.broker.NumPartitions(name)})
		}
	}
	for _, t := range topics {
		mt := protocol.MetadataTopic{Name: t.name}
		for p := int32(0); p < t.np; p++ {
			leader, epoch, replicas, isr := h.broker.PartitionMeta(t.name, p)
			mt.Partitions = append(mt.Partitions, protocol.MetadataPartition{
				Index:       p,
				Leader:      leader,
				LeaderEpoch: epoch,
				Replicas:    replicas,
				Isr:         isr,
			})
		}
		resp.Topics = append(resp.Topics, mt)
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) produce(version int16, r *kbytes.Reader) ([]byte, bool) {
	var req protocol.ProduceRequest
	req.Decode(r, version)

	resp := protocol.ProduceResponse{}
	// For acks=all we must hold the response until the data is committed (HWM has caught
	// up to it). Remember each appended partition's slot in resp and the LEO it reached.
	var waits []produceWait
	for ti := range req.Topics {
		t := req.Topics[ti]
		start := time.Now()
		tr := protocol.ProduceTopicResp{Name: t.Name}
		var topicBytes int
		for _, p := range t.Partitions {
			pr := protocol.ProducePartitionResp{Index: p.Index, BaseOffset: -1}
			log, code := h.leaderLog(t.Name, p.Index)
			if code != protocol.ErrNone {
				pr.ErrorCode = code
			} else {
				base := int64(-1)
				_, err := record.Iterate(p.Records, func(batch []byte) error {
					off, aerr := log.Append(batch)
					if base < 0 {
						base = off
					}
					return aerr
				})
				if err != nil {
					slog.Warn("produce append failed", "topic", t.Name, "partition", p.Index, "err", err)
				}
				pr.BaseOffset = base
				pr.LogStartOffset = log.EarliestOffset()
				topicBytes += len(p.Records)
				if req.Acks == -1 && err == nil && base >= 0 {
					waits = append(waits, produceWait{log: log, target: log.LatestOffset(), ti: ti, pi: len(tr.Partitions)})
				}
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		metrics.ProduceRequests.WithLabelValues(t.Name).Inc()
		metrics.ProduceBytes.WithLabelValues(t.Name).Add(float64(topicBytes))
		metrics.ProduceLatency.WithLabelValues(t.Name).Observe(time.Since(start).Seconds())
		resp.Topics = append(resp.Topics, tr)
	}
	if req.Acks == 0 {
		return nil, false // fire-and-forget: no response expected
	}
	if req.Acks == -1 {
		h.awaitCommit(waits, resp.Topics, req.TimeoutMs)
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) }), true
}

// maxProduceWait caps how long an acks=all produce blocks waiting for the ISR to commit,
// regardless of the client's request timeout.
const maxProduceWait = 5 * time.Second

// produceWait records an appended partition the acks=all path must wait on: its log, the
// LEO it must commit to (target), and its slot in the response (ti/pi) to flag on timeout.
type produceWait struct {
	log    logHandle
	target int64
	ti     int
	pi     int
}

// awaitCommit blocks until every waited partition's high watermark reaches its target
// offset or the (capped) produce timeout elapses, flagging any laggards REQUEST_TIMED_OUT.
// At RF=1 the HWM already equals the LEO, so this returns immediately.
func (h *Handler) awaitCommit(waits []produceWait, topics []protocol.ProduceTopicResp, timeoutMs int32) {
	if len(waits) == 0 {
		return
	}
	wait := maxProduceWait
	if timeoutMs > 0 && time.Duration(timeoutMs)*time.Millisecond < wait {
		wait = time.Duration(timeoutMs) * time.Millisecond
	}
	deadline := time.Now().Add(wait)
	for _, wt := range waits {
		if committed(wt.log, wt.target, deadline) {
			continue
		}
		pr := &topics[wt.ti].Partitions[wt.pi]
		pr.ErrorCode = protocol.ErrRequestTimedOut
		pr.BaseOffset = -1
	}
}

// committed polls the log's high watermark until it reaches target or the deadline passes.
func committed(log logHandle, target int64, deadline time.Time) bool {
	for {
		if log.HighWatermark() >= target {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		sleep := 20 * time.Millisecond
		if remaining < sleep {
			sleep = remaining
		}
		time.Sleep(sleep)
	}
}

// maxFetchWait caps how long a fetch will long-poll, regardless of the client's
// requested max.wait.ms.
const maxFetchWait = 5 * time.Second

func (h *Handler) fetch(version int16, r *kbytes.Reader) []byte {
	var req protocol.FetchRequest
	req.Decode(r, version)

	// Long-poll: hold the request until at least min_bytes of data are available or
	// max_wait_ms elapses (Kafka semantics). Without this, caught-up consumers
	// busy-poll the broker thousands of times per second.
	wait := time.Duration(req.MaxWaitMs) * time.Millisecond
	if wait > maxFetchWait {
		wait = maxFetchWait
	}
	minBytes := req.MinBytes
	if minBytes < 1 {
		minBytes = 1
	}
	deadline := time.Now().Add(wait)

	for {
		start := time.Now()
		resp, total, hardErr := h.buildFetch(&req, version)
		if hardErr || total >= int(minBytes) || wait <= 0 || !time.Now().Before(deadline) {
			// Record per-topic fetch metrics on the final response.
			for _, t := range req.Topics {
				metrics.FetchRequests.WithLabelValues(t.Name).Inc()
				metrics.FetchBytes.WithLabelValues(t.Name).Add(float64(total))
				metrics.FetchLatency.WithLabelValues(t.Name).Observe(time.Since(start).Seconds())
			}
			return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
		}
		remaining := time.Until(deadline)
		sleep := 20 * time.Millisecond
		if remaining < sleep {
			sleep = remaining
		}
		time.Sleep(sleep)
	}
}

// buildFetch reads every requested partition once and returns the response, the total
// number of record bytes gathered (used to decide whether to keep long-polling), and
// whether any partition hit a terminal error (e.g. OFFSET_OUT_OF_RANGE) that should
// end the long-poll immediately rather than wait for data that will never arrive.
func (h *Handler) buildFetch(req *protocol.FetchRequest, version int16) (resp protocol.FetchResponse, total int, hardErr bool) {
	isReplica := req.ReplicaID >= 0
	for _, t := range req.Topics {
		tr := protocol.FetchTopicResp{Name: t.Name}
		for _, p := range t.Partitions {
			pr := protocol.FetchPartitionResp{Partition: p.Partition}
			log, code := h.leaderLog(t.Name, p.Partition)
			if code != protocol.ErrNone {
				pr.ErrorCode = code
			} else {
				maxBytes := p.MaxBytes
				if maxBytes <= 0 {
					maxBytes = defaultFetchBytes
				}
				data, err := log.Read(p.FetchOffset, maxBytes)
				switch {
				case errors.Is(err, storage.ErrOffsetOutOfRange):
					pr.ErrorCode = protocol.ErrOffsetOutOfRange
					hardErr = true
				case err != nil:
					slog.Warn("fetch read failed", "topic", t.Name, "partition", p.Partition, "err", err)
				}
				hwm := log.HighWatermark()
				if isReplica {
					// A follower replicating from this leader: serve up to the LEO and record
					// its progress so the leader can advance the HWM / maintain the ISR.
					h.broker.RecordReplicaFetch(t.Name, p.Partition, req.ReplicaID, p.FetchOffset)
				} else {
					// A consumer may only see committed records: drop anything at or above
					// the high watermark (it lands on a batch boundary, so this is exact).
					data = clampToHighWatermark(data, hwm)
				}
				pr.HighWatermark = hwm
				pr.LastStable = hwm
				pr.LogStartOffset = log.EarliestOffset()
				pr.Records = data
				total += len(data)
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp, total, hardErr
}

// errClampStop ends the clamp walk once a batch reaches the high watermark.
var errClampStop = errors.New("clamp: at high watermark")

// clampToHighWatermark trims a record set to the batches lying entirely below hwm. The HWM
// always sits on a batch boundary (followers replicate whole batches), so a batch is either
// fully committed (lastOffset < hwm) or not yet — there is never a partial batch to split.
func clampToHighWatermark(data []byte, hwm int64) []byte {
	if len(data) == 0 {
		return data
	}
	keep := 0
	record.Iterate(data, func(batch []byte) error {
		h, err := record.ParseHeader(batch)
		if err != nil {
			return err
		}
		if h.LastOffset() >= hwm {
			return errClampStop
		}
		keep += h.TotalSize()
		return nil
	})
	return data[:keep]
}

func (h *Handler) listOffsets(version int16, r *kbytes.Reader) []byte {
	var req protocol.ListOffsetsRequest
	req.Decode(r, version)

	resp := protocol.ListOffsetsResponse{}
	for _, t := range req.Topics {
		tr := protocol.ListOffsetsTopicResp{Name: t.Name}
		for _, p := range t.Partitions {
			pr := protocol.ListOffsetsPartitionResp{Index: p.Index, Timestamp: -1}
			log, code := h.leaderLog(t.Name, p.Index)
			switch {
			case code != protocol.ErrNone:
				pr.ErrorCode = code
			case p.Timestamp == protocol.TimestampEarliest:
				pr.Offset = log.EarliestOffset()
			case p.Timestamp == protocol.TimestampLatest:
				pr.Offset = log.HighWatermark() // consumers see up to the committed offset
			default:
				// Time-based seek: first offset with timestamp >= the requested time.
				if off, ok := log.OffsetForTimestamp(p.Timestamp); ok {
					pr.Offset = off
				} else {
					pr.Offset = -1 // no record at or after the requested timestamp
				}
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) findCoordinator(version int16, r *kbytes.Reader) []byte {
	var req protocol.FindCoordinatorRequest
	req.Decode(r, version)
	cl := h.broker.Cluster()
	coordID := cl.GroupCoordinator(req.Key)
	resp := protocol.FindCoordinatorResponse{NodeID: coordID}
	if bi, ok := cl.Broker(coordID); ok {
		resp.Host = bi.Host
		resp.Port = bi.Port
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) joinGroup(version int16, r *kbytes.Reader) []byte {
	var req protocol.JoinGroupRequest
	req.Decode(r, version)
	if h.notCoordinator(req.GroupID) {
		resp := protocol.JoinGroupResponse{ErrorCode: protocol.ErrNotCoordinator}
		return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
	}

	protocols := make([]group.Protocol, len(req.Protocols))
	for i, p := range req.Protocols {
		protocols[i] = group.Protocol{Name: p.Name, Metadata: p.Metadata}
	}
	res := h.coord.Join(group.JoinArgs{
		GroupID:            req.GroupID,
		MemberID:           req.MemberID,
		ClientID:           req.GroupID, // member-id prefix; group id is a stable, readable choice
		ProtocolType:       req.ProtocolType,
		Protocols:          protocols,
		SessionTimeoutMs:   req.SessionTimeout,
		RebalanceTimeoutMs: req.RebalanceTimeout,
	})
	resp := protocol.JoinGroupResponse{
		ErrorCode:    res.ErrorCode,
		GenerationID: res.GenerationID,
		ProtocolName: res.ProtocolName,
		Leader:       res.LeaderID,
		MemberID:     res.MemberID,
	}
	for _, m := range res.Members {
		resp.Members = append(resp.Members, protocol.JoinGroupMember{MemberID: m.ID, Metadata: m.Metadata})
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) syncGroup(version int16, r *kbytes.Reader) []byte {
	var req protocol.SyncGroupRequest
	req.Decode(r, version)
	if h.notCoordinator(req.GroupID) {
		resp := protocol.SyncGroupResponse{ErrorCode: protocol.ErrNotCoordinator}
		return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
	}
	assignments := make(map[string][]byte, len(req.Assignments))
	for _, a := range req.Assignments {
		assignments[a.MemberID] = a.Assignment
	}
	res := h.coord.Sync(req.GroupID, req.MemberID, req.GenerationID, assignments)
	resp := protocol.SyncGroupResponse{ErrorCode: res.ErrorCode, Assignment: res.Assignment}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) heartbeat(version int16, r *kbytes.Reader) []byte {
	var req protocol.HeartbeatRequest
	req.Decode(r, version)
	code := protocol.ErrNotCoordinator
	if !h.notCoordinator(req.GroupID) {
		code = h.coord.Heartbeat(req.GroupID, req.MemberID, req.GenerationID)
	}
	resp := protocol.HeartbeatResponse{ErrorCode: code}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) leaveGroup(version int16, r *kbytes.Reader) []byte {
	var req protocol.LeaveGroupRequest
	req.Decode(r, version)
	code := protocol.ErrNotCoordinator
	if !h.notCoordinator(req.GroupID) {
		code = h.coord.Leave(req.GroupID, req.MemberID)
	}
	resp := protocol.LeaveGroupResponse{ErrorCode: code}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

// listGroups returns the groups this broker coordinates. In a cluster a client lists
// every broker and merges, so returning only locally-coordinated groups is correct.
func (h *Handler) listGroups(version int16, r *kbytes.Reader) []byte {
	var req protocol.ListGroupsRequest
	req.Decode(r, version)
	resp := protocol.ListGroupsResponse{}
	for _, g := range h.coord.ListGroups() {
		if h.notCoordinator(g.GroupID) {
			continue
		}
		resp.Groups = append(resp.Groups, protocol.ListGroupsGroup{GroupID: g.GroupID, ProtocolType: g.ProtocolType})
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

// describeGroups returns member/state details for the requested groups. A group this
// broker does not coordinate is rejected with NOT_COORDINATOR (the client reaches the
// coordinator via FindCoordinator first); an unknown group is reported as "Dead".
func (h *Handler) describeGroups(version int16, r *kbytes.Reader) []byte {
	var req protocol.DescribeGroupsRequest
	req.Decode(r, version)
	resp := protocol.DescribeGroupsResponse{}
	for _, id := range req.Groups {
		dg := protocol.DescribeGroupsGroup{GroupID: id}
		switch {
		case h.notCoordinator(id):
			dg.ErrorCode = protocol.ErrNotCoordinator
		default:
			desc, ok := h.coord.DescribeGroup(id)
			if !ok {
				dg.State = "Dead"
			} else {
				dg.State = desc.State
				dg.ProtocolType = desc.ProtocolType
				dg.Protocol = desc.Protocol
				for _, m := range desc.Members {
					// ClientID/ClientHost are intentionally empty (see docs/GAPS_PLAN.md).
					dg.Members = append(dg.Members, protocol.DescribeGroupsMember{
						MemberID:   m.MemberID,
						Metadata:   m.Metadata,
						Assignment: m.Assignment,
					})
				}
			}
		}
		resp.Groups = append(resp.Groups, dg)
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) offsetCommit(version int16, r *kbytes.Reader) []byte {
	var req protocol.OffsetCommitRequest
	req.Decode(r, version)
	notCoord := h.notCoordinator(req.GroupID)
	store := h.coord.Store()
	resp := protocol.OffsetCommitResponse{}
	for _, t := range req.Topics {
		tr := protocol.OffsetCommitTopicResp{Name: t.Name}
		for _, p := range t.Partitions {
			meta := ""
			if p.Metadata != nil {
				meta = *p.Metadata
			}
			code := protocol.ErrNone
			switch {
			case notCoord:
				code = protocol.ErrNotCoordinator
			default:
				if err := store.Commit(req.GroupID, t.Name, p.Index, p.Offset, meta); err != nil {
					slog.Warn("offset commit failed", "group", req.GroupID, "err", err)
					code = protocol.ErrUnknownTopicOrPart
				}
			}
			tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResp{Index: p.Index, ErrorCode: code})
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) offsetFetch(version int16, r *kbytes.Reader) []byte {
	var req protocol.OffsetFetchRequest
	req.Decode(r, version)
	if h.notCoordinator(req.GroupID) {
		resp := protocol.OffsetFetchResponse{ErrorCode: protocol.ErrNotCoordinator}
		return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
	}
	store := h.coord.Store()

	type want struct {
		name  string
		parts []int32
	}
	var wants []want
	if req.AllTopics {
		for _, t := range h.broker.KnownTopics() {
			parts := make([]int32, t.Partitions)
			for i := range parts {
				parts[i] = int32(i)
			}
			wants = append(wants, want{t.Name, parts})
		}
	} else {
		for _, t := range req.Topics {
			wants = append(wants, want{t.Name, t.Partitions})
		}
	}

	resp := protocol.OffsetFetchResponse{}
	for _, wt := range wants {
		tr := protocol.OffsetFetchTopicResp{Name: wt.name}
		for _, pid := range wt.parts {
			off, meta, found := store.Fetch(req.GroupID, wt.name, pid)
			if !found {
				off = -1
			}
			var metaPtr *string
			if meta != "" {
				metaPtr = &meta
			}
			tr.Partitions = append(tr.Partitions, protocol.OffsetFetchPartitionResp{
				Index:       pid,
				Offset:      off,
				LeaderEpoch: -1,
				Metadata:    metaPtr,
			})
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) createTopics(version int16, r *kbytes.Reader) []byte {
	var req protocol.CreateTopicsRequest
	req.Decode(r, version)
	resp := protocol.CreateTopicsResponse{}
	for _, t := range req.Topics {
		tr := protocol.CreateTopicsTopicResp{Name: t.Name}
		if err := h.broker.CreateTopic(t.Name, t.NumPartitions); err != nil {
			tr.ErrorCode = protocol.ErrTopicAlreadyExists
			msg := err.Error()
			tr.ErrorMessage = &msg
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

func (h *Handler) createPartitions(version int16, r *kbytes.Reader) []byte {
	var req protocol.CreatePartitionsRequest
	req.Decode(r, version)
	resp := protocol.CreatePartitionsResponse{}
	for _, t := range req.Topics {
		tr := protocol.CreatePartitionsTopicResp{Name: t.Name}
		if err := h.broker.CreatePartitions(t.Name, t.Count); err != nil {
			tr.ErrorCode = protocol.ErrInvalidPartitions
			msg := err.Error()
			tr.ErrorMessage = &msg
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return encode(func(w *kbytes.Writer) { resp.Encode(w, version) })
}

// notCoordinator reports whether this broker is NOT the coordinator for the group;
// when true, group requests are rejected with NOT_COORDINATOR so the client
// re-discovers the right broker via FindCoordinator.
func (h *Handler) notCoordinator(groupID string) bool {
	return !h.broker.Cluster().IsCoordinator(groupID)
}

// leaderLog resolves the local log for a partition this broker leads, returning a
// Kafka error code instead of the log when it cannot: NOT_LEADER if another broker
// leads the partition (client refetches metadata), or UNKNOWN_TOPIC_OR_PARTITION.
func (h *Handler) leaderLog(topic string, partition int32) (logHandle, int16) {
	log, err := h.broker.LocalLog(topic, partition)
	switch {
	case err == nil:
		return log, protocol.ErrNone
	case errors.Is(err, broker.ErrNotLeader):
		return nil, protocol.ErrNotLeader
	default:
		return nil, protocol.ErrUnknownTopicOrPart
	}
}

// logHandle is the subset of *storage.Log the handlers use (DIP / testability).
type logHandle interface {
	Append(batch []byte) (int64, error)
	Read(offset int64, maxBytes int32) ([]byte, error)
	EarliestOffset() int64
	LatestOffset() int64
	HighWatermark() int64
	OffsetForTimestamp(ts int64) (int64, bool)
}
