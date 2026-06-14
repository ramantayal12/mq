package storage

// Backend is the per-partition storage contract the broker and replication layer depend
// on. It is the full method surface of *Log, extracted so an alternative implementation
// (e.g. an object-store-backed log) can be swapped in without touching callers. The
// file-based *Log is the default implementation.
type Backend interface {
	// Append assigns the next offset to batch (patching its header + CRC) and writes it.
	Append(batch []byte) (baseOffset int64, err error)
	// AppendReplica writes a leader-assigned batch verbatim (follower write path).
	AppendReplica(batch []byte) (leo int64, err error)
	// Read returns whole record batches starting at offset, up to maxBytes.
	Read(offset int64, maxBytes int32) ([]byte, error)

	// HighWatermark is the committed offset consumers may read below.
	HighWatermark() int64
	// SetHighWatermark advances the committed offset (clamped, monotonic).
	SetHighWatermark(hwm int64)
	// HoldHighWatermark switches the log into replication mode (HWM driven externally).
	HoldHighWatermark()

	// OffsetForTimestamp returns the base offset of the first batch at or after ts.
	OffsetForTimestamp(ts int64) (offset int64, found bool)
	// EarliestOffset is the oldest retained offset.
	EarliestOffset() int64
	// LatestOffset is the log end offset (next offset to assign).
	LatestOffset() int64
	// Size is the total stored size in bytes.
	Size() int64

	// Flush durably persists buffered writes.
	Flush() error
	// EnforceRetention deletes data older than maxAgeMs or beyond maxBytes.
	EnforceRetention(maxAgeMs, maxBytes int64) error
	// Close releases all resources.
	Close() error
}

// *Log is the default file-based implementation of Backend.
var _ Backend = (*Log)(nil)
