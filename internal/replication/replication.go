// Package replication runs the follower side of partition replication. For every
// partition this broker replicates but does not lead, a fetcher goroutine acts as a
// Kafka client: it issues Fetch requests to the current leader with ReplicaID set to its
// own node id, appends the returned batches to the local log preserving the leader's
// offsets, and tracks the leader-reported high watermark. This reuses the broker's own
// wire protocol as the replication transport (GAPS_PLAN decision) — no separate RPC.
//
// The leader side (recording follower progress, advancing the HWM, ISR maintenance) lives
// in the broker, which owns the controller and the logs; this package is purely the
// follower's fetch loop, kept dependency-light (it imports only storage and the wire
// codecs) so the broker can wire it in without an import cycle.
package replication

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"mq/internal/kbytes"
	"mq/internal/protocol"
	"mq/internal/record"
	"mq/internal/storage"
)

const (
	fetchAPIVersion = 4               // carries replica_id, isolation_level, hwm, last_stable; pre-flexible
	fetchMaxWaitMs  = 500             // long-poll the leader so a caught-up follower idles cheaply
	fetchMinBytes   = 1               // return as soon as any byte is available
	fetchMaxBytes   = 1 << 20         // per fetch
	dialTimeout     = 3 * time.Second // connecting to the leader
	readTimeout     = 5 * time.Second // bound a single fetch round-trip (> fetchMaxWaitMs)
	retryBackoff    = 200 * time.Millisecond
)

// LeaderResolver reports the current leader's node id and Kafka address for a partition.
// ok is false when no leader is known yet (the fetcher then backs off and retries).
type LeaderResolver func(topic string, p int32) (nodeID int32, addr string, ok bool)

// LogResolver returns the local follower log for a partition (opening it if needed).
type LogResolver func(topic string, p int32) (storage.Backend, error)

type tp struct {
	topic string
	p     int32
}

// Manager owns one fetcher per followed partition.
type Manager struct {
	self     int32
	leaderOf LeaderResolver
	logOf    LogResolver

	mu       sync.Mutex
	fetchers map[tp]*fetcher
	closed   bool
}

// NewManager builds a replication manager for node self.
func NewManager(self int32, leaderOf LeaderResolver, logOf LogResolver) *Manager {
	return &Manager{self: self, leaderOf: leaderOf, logOf: logOf, fetchers: map[tp]*fetcher{}}
}

// EnsureFollowing starts a fetcher for (topic, p) if one is not already running.
func (m *Manager) EnsureFollowing(topic string, p int32) {
	key := tp{topic, p}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	if _, ok := m.fetchers[key]; ok {
		return
	}
	f := &fetcher{m: m, topic: topic, p: p, stop: make(chan struct{}), done: make(chan struct{})}
	m.fetchers[key] = f
	go f.run()
}

// StopFollowing stops the fetcher for (topic, p) if one is running (e.g. this node became
// the partition's leader). Blocks until the goroutine exits.
func (m *Manager) StopFollowing(topic string, p int32) {
	key := tp{topic, p}
	m.mu.Lock()
	f, ok := m.fetchers[key]
	if ok {
		delete(m.fetchers, key)
	}
	m.mu.Unlock()
	if ok {
		f.shutdown()
	}
}

// Close stops every fetcher and blocks until they exit.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	fs := make([]*fetcher, 0, len(m.fetchers))
	for _, f := range m.fetchers {
		fs = append(fs, f)
	}
	m.fetchers = map[tp]*fetcher{}
	m.mu.Unlock()
	for _, f := range fs {
		f.shutdown()
	}
}

// fetcher replicates one partition from its current leader.
type fetcher struct {
	m     *Manager
	topic string
	p     int32
	stop  chan struct{}
	done  chan struct{}
}

func (f *fetcher) shutdown() {
	close(f.stop)
	<-f.done
}

func (f *fetcher) stopped() bool {
	select {
	case <-f.stop:
		return true
	default:
		return false
	}
}

// sleep waits d or until stopped; it reports whether the fetcher should exit.
func (f *fetcher) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-f.stop:
		return true
	case <-t.C:
		return false
	}
}

// run resolves the leader and replicates from it, re-resolving and backing off whenever a
// session ends (leader change, transport error, or the leader rejecting our fetch).
func (f *fetcher) run() {
	defer close(f.done)
	for {
		if f.stopped() {
			return
		}
		nodeID, addr, ok := f.m.leaderOf(f.topic, f.p)
		if ok && nodeID != f.m.self {
			f.session(addr)
		}
		if f.sleep(retryBackoff) {
			return
		}
	}
}

// session opens one connection to the leader and replicates in a loop until it must be
// torn down: the fetcher is stopped, leadership moves, or a transport/protocol error.
func (f *fetcher) session(addr string) {
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return
	}
	defer conn.Close()

	var corr int32
	for {
		if f.stopped() {
			return
		}
		// Bail if leadership moved off this address while we held the connection.
		if nodeID, cur, ok := f.m.leaderOf(f.topic, f.p); !ok || nodeID == f.m.self || cur != addr {
			return
		}
		log, err := f.m.logOf(f.topic, f.p)
		if err != nil {
			return
		}
		corr++
		req := encodeFetch(f.m.self, f.topic, f.p, log.LatestOffset(), corr)
		_ = conn.SetWriteDeadline(time.Now().Add(readTimeout))
		if _, err := conn.Write(req); err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		res, err := readFetchResponse(conn)
		if err != nil {
			return
		}
		if res.errorCode != protocol.ErrNone {
			return // not-leader / unknown-topic: re-resolve from run()
		}
		if len(res.records) > 0 {
			var applyErr error
			record.Iterate(res.records, func(batch []byte) error {
				if _, e := log.AppendReplica(batch); e != nil {
					applyErr = e
					return e
				}
				return nil
			})
			if applyErr != nil {
				return // gap/corruption: resync on the next session from the current LEO
			}
		}
		log.SetHighWatermark(res.hwm)
	}
}

// encodeFetch builds a framed Fetch v4 request for a single partition.
func encodeFetch(replicaID int32, topic string, p int32, fetchOffset int64, corr int32) []byte {
	w := kbytes.NewWriter()
	// request header v1
	w.Int16(protocol.APIFetch)
	w.Int16(fetchAPIVersion)
	w.Int32(corr)
	clientID := "mq-replica"
	w.NullableString(&clientID)
	// body v4
	w.Int32(replicaID)
	w.Int32(fetchMaxWaitMs)
	w.Int32(fetchMinBytes)
	w.Int32(fetchMaxBytes) // max_bytes (v3+)
	w.Int8(0)              // isolation_level READ_UNCOMMITTED (v4+)
	w.ArrayLen(1)
	w.String(topic)
	w.ArrayLen(1)
	w.Int32(p)
	w.Int64(fetchOffset)
	w.Int32(fetchMaxBytes) // partition max_bytes
	payload := w.Finish()

	out := binary.BigEndian.AppendUint32(make([]byte, 0, len(payload)+4), uint32(len(payload)))
	return append(out, payload...)
}

// fetchPartitionResult is the single-partition slice of a Fetch v4 response we consume.
type fetchPartitionResult struct {
	errorCode int16
	hwm       int64
	records   []byte
}

// readFetchResponse reads one framed response and decodes the first partition of the
// first topic (the fetcher only ever requests one). The response header is v0 (a bare
// correlation id), matching what the server writes.
func readFetchResponse(conn net.Conn) (fetchPartitionResult, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return fetchPartitionResult{}, err
	}
	size := binary.BigEndian.Uint32(lenBuf[:])
	frame := make([]byte, size)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return fetchPartitionResult{}, err
	}
	r := kbytes.NewReader(frame)
	r.Int32() // correlation id (response header v0)

	r.Int32() // throttle_time_ms (v1+)
	var res fetchPartitionResult
	nt := r.ArrayLen()
	for i := 0; i < nt; i++ {
		_ = r.String() // topic name
		np := r.ArrayLen()
		for j := 0; j < np; j++ {
			r.Int32()          // partition index
			ec := r.Int16()    // error code
			hwm := r.Int64()   // high watermark
			r.Int64()          // last_stable_offset (v4+)
			na := r.ArrayLen() // aborted_transactions (v4+)
			for k := 0; k < na; k++ {
				r.Int64() // producer_id
				r.Int64() // first_offset
			}
			data := r.Bytes() // record set
			if i == 0 && j == 0 {
				res = fetchPartitionResult{errorCode: ec, hwm: hwm, records: data}
			}
		}
	}
	if err := r.Err(); err != nil {
		return fetchPartitionResult{}, err
	}
	return res, nil
}
