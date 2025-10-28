package faster

import "errors"

var (
	// ErrSlotNotFound is returned when a slot is not in the log
	ErrSlotNotFound = errors.New("slot not found")

	// ErrCorruptedEntry is returned when an entry fails checksum validation
	ErrCorruptedEntry = errors.New("corrupted log entry")

	// ErrNotCommitted is returned when trying to read an uncommitted entry with ReadCommittedOnly
	ErrNotCommitted = errors.New("entry not committed")

	// ErrBufferFull is returned when the mutable buffer is full
	ErrBufferFull = errors.New("mutable buffer full")

	// ErrInvalidOffset is returned when an offset is out of bounds
	ErrInvalidOffset = errors.New("invalid offset")

	// ErrClosed is returned when operating on a closed log
	ErrClosed = errors.New("log is closed")
)
