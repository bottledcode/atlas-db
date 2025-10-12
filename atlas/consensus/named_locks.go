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

package consensus

import (
	"errors"
	"sync"
)

type namedLock struct {
	name string
	mu sync.Mutex
	lock sync.Mutex
	refs int
	onRelease func()
}

func newNamedLock(name string, onRelease func()) *namedLock {
	return &namedLock{
		name: name,
		mu: sync.Mutex{},
		refs: 0,
		lock: sync.Mutex{},
		onRelease: onRelease,
	}
}

func (n *namedLock) addRef() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.refs++
	if n.refs == 0 {
		return errors.New("lock already released")
	}
	return nil
}

func (n *namedLock) release() {
	n.mu.Lock()
	n.refs--
	if n.refs == 0 {
		n.refs = -1
		n.mu.Unlock()
		n.onRelease()
	} else {
		n.mu.Unlock()
	}
}

type namedLocker struct {
	locks map[string]*namedLock
	mu sync.Mutex
}

func newNamedLocker() *namedLocker {
	return &namedLocker{
		locks: make(map[string]*namedLock),
		mu: sync.Mutex{},
	}
}

func (l *namedLocker) lock(name string) {
	l.mu.Lock()
	lock, ok := l.locks[name]
	if !ok {
		lock = newNamedLock(name, func() {
			l.mu.Lock()
			delete(l.locks, name)
			l.mu.Unlock()
		})
		l.locks[name] = lock
	}
	err := lock.addRef()
	if err != nil {
		l.mu.Unlock()
		l.lock(name)
		return
	}
	l.mu.Unlock()
	lock.lock.Lock()
}

func (l *namedLocker) unlock(name string) {
	l.mu.Lock()
	lock, ok := l.locks[name]
	if !ok {
		l.mu.Unlock()
		return
	}

	// Decrement refs while holding l.mu
	lock.mu.Lock()
	lock.refs--
	shouldCleanup := lock.refs == 0
	if shouldCleanup {
		lock.refs = -1
		delete(l.locks, name)
	}
	lock.mu.Unlock()
	l.mu.Unlock()

	lock.lock.Unlock()
}