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
	"sync"
	"time"
)

type TableOwnershipChange struct {
	OwnershipType    OwnershipType
	MigrationVersion int64
	TableVersion     int64
}

type OwnershipType int8

const (
	Owner OwnershipType = iota
	Unowned
	Change
)

type TableOwnerships struct {
	own           map[string]struct{}
	mu            sync.RWMutex
	subscriptions map[string][]chan<- TableOwnershipChange
	commitTimes   map[string]time.Time
}

var Ownership = &TableOwnerships{
	own:           map[string]struct{}{},
	subscriptions: map[string][]chan<- TableOwnershipChange{},
	commitTimes:   map[string]time.Time{},
}

func (t *TableOwnerships) Add(table string, version int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.own[table] = struct{}{}

	for _, ch := range t.subscriptions[table] {
		ch <- TableOwnershipChange{
			OwnershipType: Owner,
			TableVersion:  version,
		}
	}
}

func (t *TableOwnerships) Remove(table string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.own, table)

	for _, ch := range t.subscriptions[table] {
		ch <- TableOwnershipChange{
			OwnershipType: Unowned,
		}
	}
}

func (t *TableOwnerships) List() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	tables := make([]string, 0, len(t.own))
	for table := range t.own {
		tables = append(tables, table)
	}
	return tables
}

func (t *TableOwnerships) Commit(table string, version int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.commitTimes[table] = time.Now()

	for _, ch := range t.subscriptions[table] {
		ch <- TableOwnershipChange{
			OwnershipType:    Change,
			MigrationVersion: version,
		}
	}
}

func (t *TableOwnerships) GetCommitTime(table string) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.commitTimes[table]
}

func (t *TableOwnerships) IsOwner(table string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, ok := t.own[table]
	return ok
}

func (t *TableOwnerships) Subscribe(table string) chan TableOwnershipChange {
	t.mu.Lock()
	defer t.mu.Unlock()

	ch := make(chan TableOwnershipChange)
	t.subscriptions[table] = append(t.subscriptions[table], ch)
	return ch
}

func (t *TableOwnerships) Unsubscribe(table string, ch chan TableOwnershipChange) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, c := range t.subscriptions[table] {
		if c == ch {
			t.subscriptions[table] = append(t.subscriptions[table][:i], t.subscriptions[table][i+1:]...)
			close(c)
			break
		}
	}
}
