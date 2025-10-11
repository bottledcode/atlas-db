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

package trie

import (
	"sync"
	"testing"
)

func TestTrie_Insert(t *testing.T) {
	tr := New[string]()

	t.Run("basic insertion", func(t *testing.T) {
		tr.Insert([]byte("test"), "value1")
		prefixes := tr.PrefixesOf([]byte("test"))
		if len(prefixes) != 1 || prefixes[0] != "value1" {
			t.Errorf("expected [value1], got %v", prefixes)
		}
	})

	t.Run("multiple values for same prefix", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("key"), "value1")
		tr.Insert([]byte("key"), "value2")

		prefixes := tr.PrefixesOf([]byte("key"))
		// All values are stored in the same node and returned together
		if len(prefixes) != 2 {
			t.Errorf("expected 2 values, got %d", len(prefixes))
		}
	})

	t.Run("empty prefix", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte(""), "empty")
		// Empty prefix is stored at root, but PrefixesOf only checks nodes
		// after traversing at least one byte, so it won't match
		prefixes := tr.PrefixesOf([]byte("anything"))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes (empty prefix at root not checked), got %v", prefixes)
		}

		// However, empty input against empty prefix should work
		prefixes = tr.PrefixesOf([]byte(""))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes for empty input, got %v", prefixes)
		}
	})

	t.Run("nested prefixes", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("a"), "a")
		tr.Insert([]byte("ab"), "ab")
		tr.Insert([]byte("abc"), "abc")

		// PrefixesOf returns all matching prefixes found while traversing
		prefixes := tr.PrefixesOf([]byte("abcd"))
		if len(prefixes) != 3 {
			t.Errorf("expected 3 prefixes, got %d: %v", len(prefixes), prefixes)
		}

		// Exact match still works
		prefixes = tr.PrefixesOf([]byte("abc"))
		if len(prefixes) != 3 {
			t.Errorf("expected 3 prefixes for exact match, got %d: %v", len(prefixes), prefixes)
		}
	})
}

func TestTrie_PrefixesOf(t *testing.T) {
	t.Run("finds all matching prefixes", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("he"), "value-he")
		tr.Insert([]byte("hel"), "value-hel")
		tr.Insert([]byte("hell"), "value-hell")
		tr.Insert([]byte("hello"), "value-hello")

		prefixes := tr.PrefixesOf([]byte("hello world"))
		if len(prefixes) != 4 {
			t.Errorf("expected 4 prefixes, got %d: %v", len(prefixes), prefixes)
		}

		// Verify order matches traversal order through the trie
		expected := []string{"value-he", "value-hel", "value-hell", "value-hello"}
		for i, exp := range expected {
			if i >= len(prefixes) {
				t.Errorf("missing prefix at index %d", i)
				break
			}
			if prefixes[i] != exp {
				t.Errorf("at index %d: expected %s, got %s", i, exp, prefixes[i])
			}
		}
	})

	t.Run("no matching prefixes", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("hello"), "hello")

		prefixes := tr.PrefixesOf([]byte("world"))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes, got %v", prefixes)
		}
	})

	t.Run("partial match stops at first non-match", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("hello"), "hello")
		tr.Insert([]byte("help"), "help")

		prefixes := tr.PrefixesOf([]byte("helium"))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes (path exists but no end markers), got %v", prefixes)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("test"), "test")

		prefixes := tr.PrefixesOf([]byte(""))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes for empty input, got %v", prefixes)
		}
	})

	t.Run("input shorter than prefix", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("hello"), "hello")

		prefixes := tr.PrefixesOf([]byte("hel"))
		if len(prefixes) != 0 {
			t.Errorf("expected no complete prefixes, got %v", prefixes)
		}
	})
}

func TestTrie_LongestPrefixOf(t *testing.T) {
	t.Run("finds longest matching prefix", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("he"), "he")
		tr.Insert([]byte("hel"), "hel")
		tr.Insert([]byte("hello"), "hello")

		longest := tr.LongestPrefixOf([]byte("hello world"))
		expected := []byte("hello")
		if string(longest) != string(expected) {
			t.Errorf("expected %s, got %s", expected, longest)
		}
	})

	t.Run("returns nil when no prefix matches", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("hello"), "hello")

		longest := tr.LongestPrefixOf([]byte("world"))
		if longest != nil {
			t.Errorf("expected nil, got %v", longest)
		}
	})

	t.Run("handles path without end markers", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("he"), "he")
		tr.Insert([]byte("hello"), "hello")

		// "hel" exists as a path but not as an end marker
		longest := tr.LongestPrefixOf([]byte("helium"))
		expected := []byte("he")
		if string(longest) != string(expected) {
			t.Errorf("expected %s, got %s", expected, longest)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("test"), "test")

		longest := tr.LongestPrefixOf([]byte(""))
		if longest != nil {
			t.Errorf("expected nil for empty input, got %v", longest)
		}
	})

	t.Run("exact match", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("hello"), "hello")

		longest := tr.LongestPrefixOf([]byte("hello"))
		expected := []byte("hello")
		if string(longest) != string(expected) {
			t.Errorf("expected %s, got %s", expected, longest)
		}
	})

	t.Run("multiple prefixes of different lengths", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("a"), "a")
		tr.Insert([]byte("abc"), "abc")
		tr.Insert([]byte("abcdef"), "abcdef")

		longest := tr.LongestPrefixOf([]byte("abcdefgh"))
		expected := []byte("abcdef")
		if string(longest) != string(expected) {
			t.Errorf("expected %s, got %s", expected, longest)
		}
	})
}

func TestTrie_Remove(t *testing.T) {
	t.Run("removes existing prefix", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("test"), "value")

		removed := tr.Remove([]byte("test"))
		if !removed {
			t.Error("expected Remove to return true")
		}

		prefixes := tr.PrefixesOf([]byte("test"))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes after removal, got %v", prefixes)
		}
	})

	t.Run("returns false for non-existent prefix", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("test"), "value")

		removed := tr.Remove([]byte("nonexistent"))
		if removed {
			t.Error("expected Remove to return false for non-existent prefix")
		}
	})

	t.Run("removes leaf node but preserves parent", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("test"), "test")
		tr.Insert([]byte("testing"), "testing")

		tr.Remove([]byte("testing"))

		// "test" should still exist
		prefixes := tr.PrefixesOf([]byte("test"))
		if len(prefixes) != 1 || prefixes[0] != "test" {
			t.Errorf("expected [test], got %v", prefixes)
		}

		// "testing" should be gone
		prefixes = tr.PrefixesOf([]byte("testing"))
		if len(prefixes) != 1 {
			t.Errorf("expected only [test] (parent), got %v", prefixes)
		}
	})

	t.Run("removes parent but preserves child", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("test"), "test")
		tr.Insert([]byte("testing"), "testing")

		tr.Remove([]byte("test"))

		// "testing" should still exist
		prefixes := tr.PrefixesOf([]byte("testing"))
		if len(prefixes) != 1 || prefixes[0] != "testing" {
			t.Errorf("expected [testing], got %v", prefixes)
		}

		// "test" should not be a valid prefix anymore
		prefixes = tr.PrefixesOf([]byte("test"))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes, got %v", prefixes)
		}
	})

	t.Run("cleans up unnecessary nodes", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("testing"), "testing")

		tr.Remove([]byte("testing"))

		// Verify the trie is cleaned up by checking that nothing matches
		prefixes := tr.PrefixesOf([]byte("testing"))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes, got %v", prefixes)
		}
	})

	t.Run("handles path without end marker", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("hello"), "hello")

		// "hel" exists as a path but not as an end marker
		removed := tr.Remove([]byte("hel"))
		if removed {
			t.Error("expected Remove to return false for path without end marker")
		}

		// "hello" should still exist
		prefixes := tr.PrefixesOf([]byte("hello"))
		if len(prefixes) != 1 {
			t.Errorf("expected [hello] to still exist, got %v", prefixes)
		}
	})

	t.Run("removes prefix with multiple values", func(t *testing.T) {
		tr := New[string]()
		tr.Insert([]byte("key"), "value1")
		tr.Insert([]byte("key"), "value2")

		removed := tr.Remove([]byte("key"))
		if !removed {
			t.Error("expected Remove to return true")
		}

		// After removal, the prefix should not be findable
		prefixes := tr.PrefixesOf([]byte("key"))
		if len(prefixes) != 0 {
			t.Errorf("expected no prefixes after removal, got %v", prefixes)
		}
	})
}

func TestTrie_Concurrency(t *testing.T) {
	t.Run("concurrent inserts", func(t *testing.T) {
		tr := New[int]()
		var wg sync.WaitGroup

		// Insert 100 items concurrently
		for i := range 100 {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				key := []byte{byte(val % 10)}
				tr.Insert(key, val)
			}(i)
		}

		wg.Wait()

		// Verify all values were inserted
		for i := range 10 {
			key := []byte{byte(i)}
			values := tr.PrefixesOf(key)
			if len(values) != 10 {
				t.Errorf("key %d: expected 10 values, got %d", i, len(values))
			}
		}
	})

	t.Run("concurrent reads and writes", func(t *testing.T) {
		tr := New[string]()
		var wg sync.WaitGroup

		// Pre-populate
		for i := range 10 {
			tr.Insert([]byte{byte(i)}, "initial")
		}

		// Concurrent writers
		for i := range 50 {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				key := []byte{byte(val % 10)}
				tr.Insert(key, "concurrent")
			}(i)
		}

		// Concurrent readers
		for i := range 50 {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				key := []byte{byte(val % 10)}
				_ = tr.PrefixesOf(key)
				_ = tr.LongestPrefixOf(key)
			}(i)
		}

		wg.Wait()

		// Verify data integrity
		for i := range 10 {
			key := []byte{byte(i)}
			values := tr.PrefixesOf(key)
			if len(values) < 1 {
				t.Errorf("key %d: expected at least 1 value, got %d", i, len(values))
			}
		}
	})

	t.Run("concurrent removes and reads", func(t *testing.T) {
		tr := New[string]()
		var wg sync.WaitGroup

		// Pre-populate with more data
		for i := range 20 {
			tr.Insert([]byte{byte(i)}, "value")
		}

		// Concurrent removers
		for i := range 10 {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				key := []byte{byte(val)}
				tr.Remove(key)
			}(i)
		}

		// Concurrent readers
		for i := range 50 {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				key := []byte{byte(val % 20)}
				_ = tr.PrefixesOf(key)
			}(i)
		}

		wg.Wait()

		// Verify first 10 are removed, last 10 remain
		for i := range 10 {
			key := []byte{byte(i)}
			values := tr.PrefixesOf(key)
			if len(values) != 0 {
				t.Errorf("key %d: expected to be removed, got %v", i, values)
			}
		}

		for i := 10; i < 20; i++ {
			key := []byte{byte(i)}
			values := tr.PrefixesOf(key)
			if len(values) != 1 {
				t.Errorf("key %d: expected 1 value, got %d", i, len(values))
			}
		}
	})
}

func TestTrie_EdgeCases(t *testing.T) {
	t.Run("binary data as keys", func(t *testing.T) {
		tr := New[string]()
		key1 := []byte{0x00, 0xFF, 0xAB}
		key2 := []byte{0x00, 0xFF, 0xAB, 0xCD}

		tr.Insert(key1, "short")
		tr.Insert(key2, "long")

		prefixes := tr.PrefixesOf(key2)
		if len(prefixes) != 2 {
			t.Errorf("expected 2 prefixes for binary data, got %d", len(prefixes))
		}
	})

	t.Run("single byte keys", func(t *testing.T) {
		tr := New[string]()
		for i := range 256 {
			tr.Insert([]byte{byte(i)}, string(rune(i)))
		}

		for i := range 256 {
			values := tr.PrefixesOf([]byte{byte(i)})
			if len(values) != 1 {
				t.Errorf("byte %d: expected 1 value, got %d", i, len(values))
			}
		}
	})

	t.Run("very long prefix", func(t *testing.T) {
		tr := New[string]()
		longKey := make([]byte, 10000)
		for i := range longKey {
			longKey[i] = byte(i % 256)
		}

		tr.Insert(longKey, "long")
		prefixes := tr.PrefixesOf(longKey)
		if len(prefixes) != 1 || prefixes[0] != "long" {
			t.Errorf("expected [long], got %v", prefixes)
		}

		longest := tr.LongestPrefixOf(longKey)
		if string(longest) != string(longKey) {
			t.Error("longest prefix should match the long key")
		}
	})

	t.Run("overlapping prefixes with different types", func(t *testing.T) {
		type testStruct struct {
			ID   int
			Name string
		}

		tr := New[testStruct]()
		tr.Insert([]byte("user:"), testStruct{1, "root"})
		tr.Insert([]byte("user:123"), testStruct{2, "user123"})
		tr.Insert([]byte("user:123:profile"), testStruct{3, "profile"})

		prefixes := tr.PrefixesOf([]byte("user:123:profile:picture"))
		if len(prefixes) != 3 {
			t.Errorf("expected 3 prefixes, got %d", len(prefixes))
		}
	})

	t.Run("remove on empty trie", func(t *testing.T) {
		tr := New[string]()
		removed := tr.Remove([]byte("nonexistent"))
		if removed {
			t.Error("expected Remove to return false on empty trie")
		}
	})
}
