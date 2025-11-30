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
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	ErrRegistryClosed = errors.New("registry is closed")
)

const (
	// Entry types in the registry file
	entryTypeAdd    byte = 1
	entryTypeDelete byte = 2
)

// KeyRegistry tracks all known keys for enumeration and glob matching.
// It uses an append-only file for durability and an in-memory set for fast access.
type KeyRegistry struct {
	keys     sync.Map // map[string]struct{} - set of active keys
	file     *os.File
	filePath string
	mu       sync.Mutex // protects file writes
	closed   bool
}

// NewKeyRegistry creates or opens a key registry at the given path.
func NewKeyRegistry(path string) (*KeyRegistry, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	kr := &KeyRegistry{
		file:     file,
		filePath: path,
	}

	// Load existing entries
	if err := kr.load(); err != nil {
		_ = file.Close()
		return nil, err
	}

	return kr, nil
}

// load reads the registry file and rebuilds the in-memory set.
func (kr *KeyRegistry) load() error {
	if _, err := kr.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(kr.file)

	for {
		// Read entry type
		entryType, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Read key length (4 bytes, little endian)
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Read key
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return err
		}
		key := string(keyBytes)

		// Apply entry
		switch entryType {
		case entryTypeAdd:
			kr.keys.Store(key, struct{}{})
		case entryTypeDelete:
			kr.keys.Delete(key)
		}
	}

	return nil
}

// Register adds a key to the registry. Idempotent - registering an existing key is a no-op.
func (kr *KeyRegistry) Register(key string) error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return ErrRegistryClosed
	}

	// Check if already registered
	if _, exists := kr.keys.Load(key); exists {
		return nil
	}

	// Write to file first (durability)
	if err := kr.writeEntry(entryTypeAdd, key); err != nil {
		return err
	}

	// Then update in-memory
	kr.keys.Store(key, struct{}{})
	return nil
}

// Unregister removes a key from the registry.
func (kr *KeyRegistry) Unregister(key string) error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return ErrRegistryClosed
	}

	// Check if exists
	if _, exists := kr.keys.Load(key); !exists {
		return nil
	}

	// Write to file first (durability)
	if err := kr.writeEntry(entryTypeDelete, key); err != nil {
		return err
	}

	// Then update in-memory
	kr.keys.Delete(key)
	return nil
}

// writeEntry writes a single entry to the registry file.
func (kr *KeyRegistry) writeEntry(entryType byte, key string) error {
	keyBytes := []byte(key)
	keyLen := uint32(len(keyBytes))

	// Write entry type
	if _, err := kr.file.Write([]byte{entryType}); err != nil {
		return err
	}

	// Write key length
	if err := binary.Write(kr.file, binary.LittleEndian, keyLen); err != nil {
		return err
	}

	// Write key
	if _, err := kr.file.Write(keyBytes); err != nil {
		return err
	}

	// Sync to disk
	return kr.file.Sync()
}

// Contains checks if a key exists in the registry.
func (kr *KeyRegistry) Contains(key string) bool {
	_, exists := kr.keys.Load(key)
	return exists
}

// List returns all keys in the registry.
func (kr *KeyRegistry) List() []string {
	var keys []string
	kr.keys.Range(func(key, _ any) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}

// Count returns the number of keys in the registry.
func (kr *KeyRegistry) Count() int {
	count := 0
	kr.keys.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// Glob returns all keys matching the given glob pattern.
// Supports: * (any chars), ? (single char), [abc] (char class), [a-z] (range)
func (kr *KeyRegistry) Glob(pattern string) []string {
	keys := kr.List()
	if len(keys) == 0 {
		return nil
	}

	// For small key sets, just do sequential matching
	if len(keys) < 1000 {
		return kr.globSequential(keys, pattern)
	}

	// For larger sets, use parallel matching
	return kr.globParallel(keys, pattern)
}

func (kr *KeyRegistry) globSequential(keys []string, pattern string) []string {
	var matches []string
	for _, key := range keys {
		if globMatch(pattern, key) {
			matches = append(matches, key)
		}
	}
	return matches
}

func (kr *KeyRegistry) globParallel(keys []string, pattern string) []string {
	numWorkers := min(runtime.NumCPU(),
		// Cap at 8 workers
		8)

	chunkSize := (len(keys) + numWorkers - 1) / numWorkers
	results := make(chan []string, numWorkers)

	var wg sync.WaitGroup
	for i := range numWorkers {
		start := i * chunkSize
		end := min(start+chunkSize, len(keys))
		if start >= len(keys) {
			break
		}

		wg.Add(1)
		go func(chunk []string) {
			defer wg.Done()
			var matches []string
			for _, key := range chunk {
				if globMatch(pattern, key) {
					matches = append(matches, key)
				}
			}
			results <- matches
		}(keys[start:end])
	}

	// Close results channel when all workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var allMatches []string
	for matches := range results {
		allMatches = append(allMatches, matches...)
	}

	return allMatches
}

// globMatch matches a key against a glob pattern.
// This is a simple implementation that handles *, ?, and character classes.
func globMatch(pattern, key string) bool {
	return globMatchRecursive(pattern, key)
}

func globMatchRecursive(pattern, str string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Skip consecutive stars
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true // Trailing * matches everything
			}
			// Try matching * with 0, 1, 2, ... characters
			for i := 0; i <= len(str); i++ {
				if globMatchRecursive(pattern, str[i:]) {
					return true
				}
			}
			return false

		case '?':
			if len(str) == 0 {
				return false
			}
			pattern = pattern[1:]
			str = str[1:]

		case '[':
			if len(str) == 0 {
				return false
			}
			// Find closing bracket
			end := 1
			for end < len(pattern) && pattern[end] != ']' {
				end++
			}
			if end >= len(pattern) {
				return false // No closing bracket
			}

			// Check if char matches class
			class := pattern[1:end]
			matched := matchCharClass(class, str[0])
			if !matched {
				return false
			}
			pattern = pattern[end+1:]
			str = str[1:]

		default:
			if len(str) == 0 || pattern[0] != str[0] {
				return false
			}
			pattern = pattern[1:]
			str = str[1:]
		}
	}

	return len(str) == 0
}

func matchCharClass(class string, ch byte) bool {
	negate := false
	if len(class) > 0 && class[0] == '!' {
		negate = true
		class = class[1:]
	}

	matched := false
	for i := 0; i < len(class); i++ {
		if i+2 < len(class) && class[i+1] == '-' {
			// Range: a-z
			if ch >= class[i] && ch <= class[i+2] {
				matched = true
				break
			}
			i += 2
		} else {
			// Single char
			if ch == class[i] {
				matched = true
				break
			}
		}
	}

	if negate {
		return !matched
	}
	return matched
}

// Compact rewrites the registry file, removing deleted entries.
// This reduces file size after many delete operations.
func (kr *KeyRegistry) Compact() error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return ErrRegistryClosed
	}

	// Create temp file
	tempPath := kr.filePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	// Write all active keys
	var writeErr error
	kr.keys.Range(func(key, _ any) bool {
		keyStr := key.(string)
		keyBytes := []byte(keyStr)
		keyLen := uint32(len(keyBytes))

		if _, err := tempFile.Write([]byte{entryTypeAdd}); err != nil {
			writeErr = err
			return false
		}
		if err := binary.Write(tempFile, binary.LittleEndian, keyLen); err != nil {
			writeErr = err
			return false
		}
		if _, err := tempFile.Write(keyBytes); err != nil {
			writeErr = err
			return false
		}
		return true
	})

	if writeErr != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		return writeErr
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		return err
	}
	_ = tempFile.Close()

	// Close old file
	_ = kr.file.Close()

	// Rename temp to main
	if err := os.Rename(tempPath, kr.filePath); err != nil {
		return err
	}

	// Reopen
	kr.file, err = os.OpenFile(kr.filePath, os.O_RDWR|os.O_APPEND, 0644)
	return err
}

// Close closes the registry file.
func (kr *KeyRegistry) Close() error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return nil
	}

	kr.closed = true
	return kr.file.Close()
}
