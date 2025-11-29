package faster

import (
	"encoding/binary"
	"hash/crc32"
)

// LogEntry represents a single consensus log entry
// This matches WPaxos semantics: slot, ballot, value, committed flag
type LogEntry struct {
	Slot      uint64
	Ballot    Ballot
	Value     []byte
	Committed bool
}

// Ballot represents a WPaxos ballot number
// Ballots are ordered first by ID, then by NodeID for tie-breaking
type Ballot struct {
	ID     uint64 // Ballot counter
	NodeID uint64 // Node that created this ballot
}

// Less returns true if this ballot is less than other
func (b Ballot) Less(other Ballot) bool {
	if b.ID != other.ID {
		return b.ID < other.ID
	}
	return b.NodeID < other.NodeID
}

// Config holds configuration for the FASTER log
type Config struct {
	// Path to the log file on disk
	Path string

	// MutableSize is the size of the in-memory mutable region for uncommitted entries
	// Larger = more uncommitted entries can be held in memory
	// Recommended: 64MB - 256MB
	MutableSize uint64

	// SegmentSize is the size of each immutable log segment
	// Larger = fewer files but slower recovery
	// Recommended: 1GB - 4GB
	SegmentSize uint64

	// NumThreads is the number of concurrent threads/goroutines expected
	// Used for epoch-based memory management
	NumThreads int

	// SyncOnCommit controls whether to fsync on every commit
	// true = durable but slower, false = fast but risk data loss on crash
	SyncOnCommit bool
}

// IterateOptions controls iteration behavior for IterateCommitted
type IterateOptions struct {
	// MinSlot is the minimum slot to iterate (0 = from beginning)
	MinSlot uint64

	// MaxSlot is the maximum slot to iterate (0 = to end)
	MaxSlot uint64

	// IncludeUncommitted includes uncommitted entries from mutable region
	// Useful for debugging or special recovery scenarios
	// Default: false (only committed entries)
	IncludeUncommitted bool

	// SkipErrors continues iteration even if individual entry reads fail
	// Useful for recovering from partial corruption
	// Default: false (stop on first error)
	SkipErrors bool
}

const (
	// entryHeaderSize is the fixed size of the entry header (without value and checksum)
	entryHeaderSize = 8 + 8 + 8 + 4 + 1 // 29 bytes

	// checksumSize is the size of the checksum trailer
	checksumSize = 4

	// mutableFlag is set in the high bit of offset to indicate mutable region
	mutableFlag uint64 = 1 << 63
)

// serializeEntry serializes a log entry to bytes
func serializeEntry(entry *LogEntry) ([]byte, error) {
	// Calculate total size: header + value + checksum
	totalSize := entryHeaderSize + len(entry.Value) + checksumSize
	buf := make([]byte, totalSize)

	// Write header
	binary.LittleEndian.PutUint64(buf[0:8], entry.Slot)
	binary.LittleEndian.PutUint64(buf[8:16], entry.Ballot.ID)
	binary.LittleEndian.PutUint64(buf[16:24], entry.Ballot.NodeID)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(len(entry.Value)))

	if entry.Committed {
		buf[28] = 1
	} else {
		buf[28] = 0
	}

	// Write value
	copy(buf[entryHeaderSize:entryHeaderSize+len(entry.Value)], entry.Value)

	// Calculate checksum over header + value (everything except checksum itself)
	checksumOffset := entryHeaderSize + len(entry.Value)
	checksum := crc32.ChecksumIEEE(buf[0:checksumOffset])
	binary.LittleEndian.PutUint32(buf[checksumOffset:checksumOffset+checksumSize], checksum)

	return buf, nil
}

// deserializeEntry deserializes a log entry from bytes
func deserializeEntry(data []byte) (*LogEntry, error) {
	if len(data) < entryHeaderSize {
		return nil, ErrCorruptedEntry
	}

	// Read header
	slot := binary.LittleEndian.Uint64(data[0:8])
	ballotID := binary.LittleEndian.Uint64(data[8:16])
	ballotNode := binary.LittleEndian.Uint64(data[16:24])
	valueLen := binary.LittleEndian.Uint32(data[24:28])
	committed := data[28] == 1

	// Validate size
	totalSize := entryHeaderSize + int(valueLen) + checksumSize
	if len(data) < totalSize {
		return nil, ErrCorruptedEntry
	}

	// Read checksum
	checksumOffset := entryHeaderSize + int(valueLen)
	storedChecksum := binary.LittleEndian.Uint32(data[checksumOffset : checksumOffset+checksumSize])

	// Verify checksum
	calculatedChecksum := crc32.ChecksumIEEE(data[0:checksumOffset])
	if storedChecksum != calculatedChecksum {
		return nil, ErrCorruptedEntry
	}

	// Read value
	value := make([]byte, valueLen)
	copy(value, data[entryHeaderSize:entryHeaderSize+int(valueLen)])

	return &LogEntry{
		Slot: slot,
		Ballot: Ballot{
			ID:     ballotID,
			NodeID: ballotNode,
		},
		Value:     value,
		Committed: committed,
	}, nil
}
