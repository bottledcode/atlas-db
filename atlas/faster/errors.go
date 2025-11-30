/*
 * This file is part of Atlas-DB.
 *
 * Atlas-DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Atlas-DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
 *
 */

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

	// ErrNoSnapshot is returned when no snapshot exists
	ErrNoSnapshot = errors.New("no snapshot found")

	// ErrCorruptedSnapshot is returned when a snapshot fails checksum validation
	ErrCorruptedSnapshot = errors.New("corrupted snapshot")

	// ErrEntryTooLarge is returned when an entry value exceeds the maximum allowed size
	ErrEntryTooLarge = errors.New("entry value too large")
)
