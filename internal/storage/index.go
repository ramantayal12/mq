package storage

import "encoding/binary"

// indexEntrySize is the fixed on-disk size of a sparse index entry:
// relativeOffset (int32) + position (int32).
const indexEntrySize = 8

// indexEntry maps a record offset (relative to the segment base) to a byte
// position within the segment's .log file.
type indexEntry struct {
	relOffset int32
	position  int32
}

func encodeIndexEntry(e indexEntry) []byte {
	b := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint32(b[0:], uint32(e.relOffset))
	binary.BigEndian.PutUint32(b[4:], uint32(e.position))
	return b
}

func decodeIndexEntries(b []byte) []indexEntry {
	n := len(b) / indexEntrySize
	out := make([]indexEntry, 0, n)
	for i := 0; i < n; i++ {
		off := i * indexEntrySize
		out = append(out, indexEntry{
			relOffset: int32(binary.BigEndian.Uint32(b[off:])),
			position:  int32(binary.BigEndian.Uint32(b[off+4:])),
		})
	}
	return out
}

// floorPosition returns the byte position of the largest index entry whose
// relOffset is <= targetRel, or 0 if none (start of segment). Entries are sorted
// by relOffset by construction, so this is a binary search.
func floorPosition(index []indexEntry, targetRel int32) int32 {
	lo, hi := 0, len(index)-1
	pos := int32(0)
	for lo <= hi {
		mid := (lo + hi) / 2
		if index[mid].relOffset <= targetRel {
			pos = index[mid].position
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return pos
}
