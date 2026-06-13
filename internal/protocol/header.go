package protocol

import "mq/internal/kbytes"

// RequestHeader is Kafka request header v1 (api_key, api_version, correlation_id,
// client_id). Every API mq accepts uses header v1 on the request side; the flexible
// header v2 is never reached because of the version caps in SupportedVersions.
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      *string
}

// DecodeRequestHeader reads a v1 request header from r.
func DecodeRequestHeader(r *kbytes.Reader) RequestHeader {
	return RequestHeader{
		APIKey:        r.Int16(),
		APIVersion:    r.Int16(),
		CorrelationID: r.Int32(),
		ClientID:      r.NullableString(),
	}
}

// WriteResponseHeader writes response header v0 (correlation id only), which is the
// only response header shape mq produces.
func WriteResponseHeader(w *kbytes.Writer, correlationID int32) {
	w.Int32(correlationID)
}
