// Package server runs the TCP accept loop and per-connection request framing for
// the Kafka wire protocol. Request handling itself lives in handlers.go.
package server

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"

	"mq/internal/broker"
	"mq/internal/group"
	"mq/internal/kbytes"
	"mq/internal/protocol"
)

// maxFrameBytes bounds a single request frame (defensive against bad input).
const maxFrameBytes = 100 << 20 // 100 MiB

// Server accepts Kafka client connections and dispatches their requests.
type Server struct {
	handler *Handler
	ln      net.Listener
	wg      sync.WaitGroup
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	closed  bool
}

// New builds a server bound to listen, backed by the given broker and coordinator.
func New(listen string, b *broker.Broker, coord *group.Coordinator) (*Server, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	return NewFromListener(ln, b, coord), nil
}

// NewFromListener builds a server over an already-bound listener (used when the
// broker must learn its ephemeral port before constructing the cluster view, e.g.
// in multi-broker tests).
func NewFromListener(ln net.Listener, b *broker.Broker, coord *group.Coordinator) *Server {
	return &Server{
		handler: &Handler{broker: b, coord: coord},
		ln:      ln,
		conns:   map[net.Conn]struct{}{},
	}
}

// Addr returns the bound listener address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Requests returns how many requests of the given API key this server has handled.
func (s *Server) Requests(apiKey int16) int64 { return s.handler.Requests(apiKey) }

// Serve accepts connections until Close is called.
func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		s.track(conn, true)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.track(conn, false)
			s.handleConn(conn)
		}()
	}
}

// Close stops accepting, closes open connections, and waits for handlers to drain.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	err := s.ln.Close()
	s.wg.Wait()
	return err
}

func (s *Server) track(c net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

// handleConn reads length-prefixed request frames sequentially and writes responses.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	lenBuf := make([]byte, 4)
	for {
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return // EOF or client closed
		}
		size := binary.BigEndian.Uint32(lenBuf)
		if size == 0 || size > maxFrameBytes {
			slog.Warn("rejecting frame", "size", size)
			return
		}
		frame := make([]byte, size)
		if _, err := io.ReadFull(conn, frame); err != nil {
			return
		}

		r := kbytes.NewReader(frame)
		hdr := protocol.DecodeRequestHeader(r)

		body, reply := s.handler.Dispatch(hdr, r)
		if !reply {
			continue // e.g. Produce with acks=0
		}
		if err := writeResponse(conn, hdr.CorrelationID, body); err != nil {
			return
		}
	}
}

// writeResponse frames "size | correlation_id | body".
func writeResponse(conn net.Conn, correlationID int32, body []byte) error {
	out := make([]byte, 0, 8+len(body))
	w := kbytes.NewWriter()
	protocol.WriteResponseHeader(w, correlationID)
	w.Raw(body)
	payload := w.Finish()

	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)))
	out = append(out, payload...)
	_, err := conn.Write(out)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return err
}
