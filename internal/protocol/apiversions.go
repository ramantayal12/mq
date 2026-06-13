package protocol

import "mq/internal/kbytes"

// ApiVersionsResponse advertises the API/version ranges the broker supports.
// Request body is empty for v0-v2, so there is no request struct.
type ApiVersionsResponse struct {
	ErrorCode    int16
	Versions     []VersionRange
	ThrottleTime int32
}

// Encode writes the response body at the given version.
func (resp *ApiVersionsResponse) Encode(w *kbytes.Writer, version int16) {
	w.Int16(resp.ErrorCode)
	w.ArrayLen(len(resp.Versions))
	for _, v := range resp.Versions {
		w.Int16(v.APIKey)
		w.Int16(v.Min)
		w.Int16(v.Max)
	}
	if version >= 1 {
		w.Int32(resp.ThrottleTime)
	}
}
