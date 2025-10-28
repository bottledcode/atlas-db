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
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/options"
)

func TestLogManagerFilenames(t *testing.T) {
	// Setup temp directory
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	lm := NewLogManager()

	testCases := []struct {
		name string
		key  []byte
	}{
		{"simple key", []byte("users")},
		{"special chars", []byte("table/with/slashes")},
		{"unicode", []byte("таблица")},
		{"binary data", []byte{0x00, 0x01, 0xFF, 0xFE}},
		{"long key", []byte(strings.Repeat("a", 1000))},
	}

	filenames := make(map[string]string)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			log, err := lm.GetLog(tc.key)
			if err != nil {
				t.Fatalf("Failed to get log: %v", err)
			}
			defer log.Close()

			// Extract filename from path
			filename := filepath.Base(log.tailFile.Name())

			t.Logf("Key %q -> filename %q", tc.key, filename)

			// Verify filename is filesystem-safe
			if strings.ContainsAny(filename, "/\\:*?\"<>|") {
				t.Errorf("Filename contains unsafe characters: %s", filename)
			}

			// Verify filename has reasonable length
			if len(filename) > 255 {
				t.Errorf("Filename too long: %d chars", len(filename))
			}

			// Verify filename ends with .log
			if !strings.HasSuffix(filename, ".log") {
				t.Errorf("Filename doesn't end with .log: %s", filename)
			}

			// Check for collisions
			if existingKey, exists := filenames[filename]; exists {
				t.Errorf("COLLISION! Keys %q and %q both map to filename %q",
					existingKey, tc.key, filename)
			}
			filenames[filename] = string(tc.key)
		})
	}

	// Verify we created the expected number of unique files
	if len(filenames) != len(testCases) {
		t.Errorf("Expected %d unique filenames, got %d", len(testCases), len(filenames))
	}
}

func TestLogManagerCaching(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	lm := NewLogManager()
	key := []byte("test-table")

	// Get log first time
	log1, err := lm.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	defer log1.Close()

	// Get log second time - should return same instance
	log2, err := lm.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}

	// Should be the exact same pointer
	if log1 != log2 {
		t.Error("Expected same log instance, got different pointers")
	}
}

func TestLogManagerCollisionResistance(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	lm := NewLogManager()

	// Generate many random keys
	numKeys := 1000
	keys := make([][]byte, numKeys)
	filenames := make(map[string]int)

	for i := 0; i < numKeys; i++ {
		// Random 32-byte keys
		key := make([]byte, 32)
		_, err := rand.Read(key)
		if err != nil {
			t.Fatalf("Failed to generate random key: %v", err)
		}
		keys[i] = key

		log, err := lm.GetLog(key)
		if err != nil {
			t.Fatalf("Failed to get log for key %d: %v", i, err)
		}
		defer log.Close()

		filename := filepath.Base(log.tailFile.Name())
		filenames[filename]++
	}

	// Check for collisions
	collisions := 0
	for filename, count := range filenames {
		if count > 1 {
			t.Errorf("Collision detected: %d keys mapped to filename %s", count, filename)
			collisions++
		}
	}

	if collisions > 0 {
		t.Errorf("Total collisions: %d out of %d keys", collisions, numKeys)
	}

	t.Logf("Successfully created %d unique files for %d keys (0 collisions)", len(filenames), numKeys)
}

func TestLogManagerFilenameFormat(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "mydb")

	lm := NewLogManager()
	key := []byte("test-table")

	log, err := lm.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	defer log.Close()

	filename := filepath.Base(log.tailFile.Name())
	t.Logf("Generated filename: %s", filename)

	// Verify format: mydb.<base32-hash>.log
	if !strings.HasPrefix(filename, "mydb.") {
		t.Errorf("Filename should start with 'mydb.': %s", filename)
	}

	if !strings.HasSuffix(filename, ".log") {
		t.Errorf("Filename should end with '.log': %s", filename)
	}

	// Extract the hash part
	parts := strings.Split(filename, ".")
	if len(parts) != 3 {
		t.Errorf("Expected format 'prefix.hash.log', got %d parts", len(parts))
	} else {
		hashPart := parts[1]

		// Base32 uses A-Z and 2-7
		for _, c := range hashPart {
			if !((c >= 'A' && c <= 'Z') || (c >= '2' && c <= '7')) {
				t.Errorf("Hash contains non-base32 character: %c in %s", c, hashPart)
			}
		}

		// Should be 32 characters (our truncation)
		if len(hashPart) != 32 {
			t.Errorf("Expected hash length 32, got %d: %s", len(hashPart), hashPart)
		}
	}
}

func TestLogManagerDeterministic(t *testing.T) {
	// Same key should always produce same filename
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	key := []byte("consistent-key")

	// Create first log manager and get filename
	lm1 := NewLogManager()
	log1, err := lm1.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	filename1 := filepath.Base(log1.tailFile.Name())
	log1.Close()

	// Create second log manager and get filename
	lm2 := NewLogManager()
	log2, err := lm2.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	filename2 := filepath.Base(log2.tailFile.Name())
	log2.Close()

	// Should produce identical filenames
	if filename1 != filename2 {
		t.Errorf("Same key produced different filenames: %s vs %s", filename1, filename2)
	}

	t.Logf("Key %q consistently maps to %s", key, filename1)
}

func TestLogManagerFileCreation(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	lm := NewLogManager()
	key := []byte("test-table")

	log, err := lm.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	defer log.Close()

	// Verify file was actually created
	filepath := log.tailFile.Name()
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		t.Errorf("Log file was not created: %s", filepath)
	}

	t.Logf("Successfully created log file: %s", filepath)
}

// Benchmark filename generation
func BenchmarkLogManagerGetLog(b *testing.B) {
	dir := b.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "bench")

	lm := NewLogManager()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("test-table")
		log, err := lm.GetLog(key)
		if err != nil {
			b.Fatalf("Failed to get log: %v", err)
		}
		if i == 0 {
			defer log.Close()
		}
	}
}

func BenchmarkLogManagerGetLogUnique(b *testing.B) {
	dir := b.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "bench")

	lm := NewLogManager()

	// Pre-generate keys
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(string(rune(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log, err := lm.GetLog(keys[i])
		if err != nil {
			b.Fatalf("Failed to get log: %v", err)
		}
		defer log.Close()
	}
}
