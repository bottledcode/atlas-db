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

import "sync"

type Trie[T any] interface {
	Insert(prefix []byte, value T)
	PrefixesOf(full []byte) []T
	LongestPrefixOf(full []byte) []byte
	Remove(prefix []byte) bool
}

type node[T any] struct {
	children map[byte]*node[T]
	end      bool
	value    []T
}

type trie[T any] struct {
	root *node[T]
	mu   sync.RWMutex
}

func New[T any]() Trie[T] {
	return &trie[T]{
		root: &node[T]{children: make(map[byte]*node[T])},
	}
}

func (t *trie[T]) Insert(prefix []byte, value T) {
	t.mu.Lock()
	defer t.mu.Unlock()

	cur := t.root
	for _, r := range prefix {
		if nxt, ok := cur.children[r]; ok {
			cur = nxt
		} else {
			nxt = &node[T]{children: make(map[byte]*node[T])}
			cur.children[r] = nxt
			cur = nxt
		}
	}
	cur.value = append(cur.value, value)
	cur.end = true
}

func (t *trie[T]) PrefixesOf(full []byte) []T {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var out []T
	cur := t.root

	for _, r := range full {
		if nxt, ok := cur.children[r]; !ok {
			break
		} else {
			cur = nxt
			if cur.end {
				out = append(out, cur.value...)
			}
		}
	}
	return out
}

func (t *trie[T]) LongestPrefixOf(full []byte) []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()

	cur := t.root
	var buf []byte
	lastMatchLen := -1

	for _, r := range full {
		if nxt, ok := cur.children[r]; !ok {
			break
		} else {
			buf = append(buf, r)
			cur = nxt
			if cur.end {
				lastMatchLen = len(buf)
			}
		}
	}
	if lastMatchLen == -1 {
		return nil
	}

	return buf[:lastMatchLen]
}

func (t *trie[T]) Remove(prefix []byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	type step struct {
		parent *node[T]
		r      byte
		cur    *node[T]
	}

	cur := t.root
	var path []step

	for _, r := range prefix {
		if nxt, ok := cur.children[r]; !ok {
			return false
		} else {
			path = append(path, step{cur, r, nxt})
			cur = nxt
		}
	}
	if !cur.end {
		return false
	}
	cur.end = false

	for i := len(path) - 1; i >= 0; i-- {
		n := path[i].cur
		if n.end || len(n.children) > 0 {
			break
		}
		delete(path[i].parent.children, path[i].r)
	}
	return true
}
