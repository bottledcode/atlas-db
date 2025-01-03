package atlas

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
	own: map[string]struct{}{},
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
