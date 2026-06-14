// Package protocol defines the Kafka request/response structs and their per-version
// wire encode/decode. It depends only on internal/kbytes.
package protocol

// API keys (subset implemented by mq).
const (
	APIProduce          int16 = 0
	APIFetch            int16 = 1
	APIListOffsets      int16 = 2
	APIMetadata         int16 = 3
	APIOffsetCommit     int16 = 8
	APIOffsetFetch      int16 = 9
	APIFindCoordinator  int16 = 10
	APIJoinGroup        int16 = 11
	APIHeartbeat        int16 = 12
	APILeaveGroup       int16 = 13
	APISyncGroup        int16 = 14
	APIDescribeGroups   int16 = 15
	APIListGroups       int16 = 16
	APIApiVersions      int16 = 18
	APICreateTopics     int16 = 19
	APICreatePartitions int16 = 37
)

// VersionRange is the inclusive [Min,Max] of an API we support. Max is held below
// each API's "flexible" (KIP-482) threshold so clients negotiate the fixed-layout
// encoding and we never need compact strings or tagged fields.
type VersionRange struct {
	APIKey int16
	Min    int16
	Max    int16
}

// SupportedVersions is the table advertised in ApiVersions responses.
var SupportedVersions = []VersionRange{
	{APIProduce, 0, 7},
	{APIFetch, 0, 11},
	{APIListOffsets, 0, 5},
	{APIMetadata, 0, 8},
	{APIOffsetCommit, 0, 6},
	{APIOffsetFetch, 0, 5},
	{APIFindCoordinator, 0, 2},
	{APIJoinGroup, 0, 4},
	{APIHeartbeat, 0, 2},
	{APILeaveGroup, 0, 2},
	{APISyncGroup, 0, 2},
	{APIDescribeGroups, 0, 2},
	{APIListGroups, 0, 2},
	{APIApiVersions, 0, 2},
	{APICreateTopics, 0, 4},
	{APICreatePartitions, 0, 1},
}

// Supported reports the version range for an API key, if implemented.
func Supported(apiKey int16) (VersionRange, bool) {
	for _, v := range SupportedVersions {
		if v.APIKey == apiKey {
			return v, true
		}
	}
	return VersionRange{}, false
}

// Kafka error codes used by mq.
const (
	ErrNone                int16 = 0
	ErrOffsetOutOfRange    int16 = 1
	ErrUnknownTopicOrPart  int16 = 3
	ErrNotLeader           int16 = 6 // NOT_LEADER_OR_FOLLOWER: client should refetch metadata
	ErrRequestTimedOut     int16 = 7 // acks=all: ISR did not commit within the produce timeout
	ErrNotCoordinator      int16 = 16
	ErrIllegalGeneration   int16 = 22
	ErrUnknownMemberID     int16 = 25
	ErrRebalanceInProgress int16 = 27
	ErrUnsupportedVersion  int16 = 35
	ErrTopicAlreadyExists  int16 = 36
	ErrInvalidPartitions   int16 = 37 // CreatePartitions: bad target count (e.g. not a growth)
)
