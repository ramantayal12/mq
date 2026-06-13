package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"mq/internal/record"
)

// segment is one pair of files (.log + .index) holding a contiguous range of
// record batches starting at baseOffset.
type segment struct {
	baseOffset    int64
	logFile       *os.File
	idxFile       *os.File
	logSize       int32
	index         []indexEntry
	bytesSinceIdx int32
	indexInterval int32
}

func segmentName(baseOffset int64) string { return fmt.Sprintf("%020d", baseOffset) }

func logPath(dir string, base int64) string {
	return filepath.Join(dir, segmentName(base)+".log")
}
func indexPath(dir string, base int64) string {
	return filepath.Join(dir, segmentName(base)+".index")
}

// createSegment creates fresh .log/.index files for baseOffset.
func createSegment(dir string, baseOffset int64, indexInterval int32) (*segment, error) {
	lf, err := os.OpenFile(logPath(dir, baseOffset), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	xf, err := os.OpenFile(indexPath(dir, baseOffset), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		lf.Close()
		return nil, err
	}
	return &segment{baseOffset: baseOffset, logFile: lf, idxFile: xf, indexInterval: indexInterval}, nil
}

// openSegment opens existing files for an already-written segment and loads its index.
func openSegment(dir string, baseOffset int64, indexInterval int32) (*segment, error) {
	lf, err := os.OpenFile(logPath(dir, baseOffset), os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := lf.Stat()
	if err != nil {
		lf.Close()
		return nil, err
	}
	xf, err := os.OpenFile(indexPath(dir, baseOffset), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		lf.Close()
		return nil, err
	}
	idxBytes, err := os.ReadFile(indexPath(dir, baseOffset))
	if err != nil {
		lf.Close()
		xf.Close()
		return nil, err
	}
	return &segment{
		baseOffset:    baseOffset,
		logFile:       lf,
		idxFile:       xf,
		logSize:       int32(st.Size()),
		index:         decodeIndexEntries(idxBytes),
		indexInterval: indexInterval,
	}, nil
}

// append writes one record batch (already offset-patched by Log) at the segment's
// current end, adding a sparse index entry when the byte interval is crossed.
func (s *segment) append(batch []byte, baseOffset int64) error {
	pos := s.logSize
	if len(s.index) == 0 || s.bytesSinceIdx >= s.indexInterval {
		e := indexEntry{relOffset: int32(baseOffset - s.baseOffset), position: pos}
		if _, err := s.idxFile.Write(encodeIndexEntry(e)); err != nil {
			return err
		}
		s.index = append(s.index, e)
		s.bytesSinceIdx = 0
	}
	if _, err := s.logFile.WriteAt(batch, int64(pos)); err != nil {
		return err
	}
	s.logSize += int32(len(batch))
	s.bytesSinceIdx += int32(len(batch))
	return nil
}

// readBatchesFrom returns whole batches starting at the first batch whose last
// offset >= wantOffset, accumulating up to maxBytes (always at least one batch).
func (s *segment) readBatchesFrom(wantOffset int64, maxBytes int32) ([]byte, error) {
	pos := floorPosition(s.index, int32(wantOffset-s.baseOffset))
	hdr := make([]byte, record.HeaderSize)
	var out []byte
	for pos < s.logSize {
		if _, err := s.logFile.ReadAt(hdr, int64(pos)); err != nil {
			return out, err
		}
		h, err := record.ParseHeader(hdr)
		if err != nil {
			return out, err
		}
		total := int32(h.TotalSize())
		if h.LastOffset() < wantOffset { // batch entirely before target
			pos += total
			continue
		}
		if len(out) > 0 && int32(len(out))+total > maxBytes {
			break
		}
		full := make([]byte, total)
		if _, err := s.logFile.ReadAt(full, int64(pos)); err != nil {
			return out, err
		}
		out = append(out, full...)
		pos += total
		if int32(len(out)) >= maxBytes {
			break
		}
	}
	return out, nil
}

func (s *segment) flush() error {
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.idxFile.Sync()
}

func (s *segment) close() error {
	err1 := s.logFile.Close()
	err2 := s.idxFile.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
