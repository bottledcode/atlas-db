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

import (
	"crypto/sha256"
	"encoding/base32"
	"strings"
	"sync"

	"github.com/bottledcode/atlas-db/atlas/options"
)

type ByteSize uint64

const (
	B  ByteSize = 1
	KB ByteSize = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

type LogManager struct {
	logs map[string]*FasterLog
	mu   sync.Mutex
}

func NewLogManager() *LogManager {
	return &LogManager{
		logs: make(map[string]*FasterLog),
	}
}

const (
	MutableSize = 64 * MB
	SegmentSize = 1 * GB
	NumThreads = 128
)

func (l *LogManager) GetLog(key []byte) (*FasterLog, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Use key directly as map key (Go handles this efficiently)
	keyStr := string(key)
	if v, ok := l.logs[keyStr]; ok {
		return v, nil
	}

	// Create filesystem-safe filename from key using SHA256 + Base32
	// SHA256 gives us cryptographic collision resistance (2^256 space)
	// Base32 encoding creates filesystem-safe alphanumeric names
	hash := sha256.Sum256(key)

	// Use base32 (not base64) to avoid case-sensitivity issues on some filesystems
	// StdEncoding uses A-Z and 2-7 (no ambiguous characters like 0/O or 1/l)
	safeKey := base32.StdEncoding.EncodeToString(hash[:])

	// Remove padding '=' characters to keep filenames cleaner
	safeKey = strings.TrimRight(safeKey, "=")

	// Truncate to reasonable length (first 32 chars of base32 = 160 bits of entropy)
	// This still gives us 2^160 unique values (way more than we'll ever need)
	if len(safeKey) > 32 {
		safeKey = safeKey[:32]
	}

	path := options.CurrentOptions.DbFilename + "." + safeKey + ".log"

	log, err := NewFasterLog(Config{
		Path:         path,
		MutableSize:  uint64(MutableSize),
		SegmentSize:  uint64(SegmentSize),
		NumThreads:   NumThreads,
		SyncOnCommit: false,
	})
	if err != nil {
		return nil, err
	}

	l.logs[keyStr] = log
	return log, nil
}
