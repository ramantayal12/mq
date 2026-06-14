package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"mq/internal/kbytes"
	"mq/internal/protocol"
)

// runHealthcheck performs a Kafka ApiVersions handshake against a running broker and
// returns a process exit code (0 = healthy, 1 = unhealthy). It is the in-binary probe
// used by the Docker HEALTHCHECK, since the distroless runtime image has no shell or
// curl/nc to script a TCP check externally.
//
// Usage: mqbroker healthcheck [host:port]   (default 127.0.0.1:9092)
func runHealthcheck(args []string) int {
	addr := "127.0.0.1:9092"
	if len(args) > 0 && args[0] != "" {
		addr = args[0]
	}

	if err := probeApiVersions(addr, 3*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	return 0
}

// probeApiVersions dials addr, sends an ApiVersions v0 request and verifies the broker
// replies with ErrorCode 0 within the deadline.
func probeApiVersions(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	if _, err := conn.Write(apiVersionsRequest()); err != nil {
		return fmt.Errorf("write request: %w", err)
	}

	resp, err := readFrame(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Response = header v0 (correlation id, int32) + ApiVersionsResponse body.
	r := kbytes.NewReader(resp)
	r.Int32() // correlation id
	errorCode := r.Int16()
	if r.Err() != nil {
		return fmt.Errorf("decode response: %w", r.Err())
	}
	if errorCode != 0 {
		return fmt.Errorf("broker returned error code %d", errorCode)
	}
	return nil
}

// apiVersionsRequest builds a length-prefixed ApiVersions v0 request (header v1 +
// empty body), mirroring what a real Kafka client sends first.
func apiVersionsRequest() []byte {
	clientID := "healthcheck"
	w := kbytes.NewWriter()
	w.Int32(0) // length placeholder, patched below
	w.Int16(protocol.APIApiVersions)
	w.Int16(0) // api version
	w.Int32(1) // correlation id
	w.NullableString(&clientID)
	w.PatchInt32(0, int32(w.Len()-4))
	return w.Finish()
}

// readFrame reads a single 4-byte length-prefixed response frame.
func readFrame(conn net.Conn) ([]byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	buf := make([]byte, size)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
