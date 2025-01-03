package atlas

import (
	"go.uber.org/zap"
	"slices"
	"sync"
	"time"
	"zombiezen.com/go/sqlite"
)

type ReadMode int8

const (
	ReadModeConsistent ReadMode = iota
	ReadModeStrong
	ReadModeEventual
	ReadModeBounded
)

type DenyReason int8

const (
	ReasonJournalModeChange DenyReason = iota // deny journal_mode changes
	ReasonAtlasWrite                          // deny writes to atlas tables
	ReasonNotAllowedLocally                   // deny strong reads locally
	ReasonOwnershipFailed                     // deny ownership requests
)

// Want represents a request for a table, with the option to request ownership
type Want struct {
	Table     string
	Ownership bool
}

type Authorizer struct {
	readMode   ReadMode
	boundTime  time.Duration
	mu         sync.RWMutex
	lastReason DenyReason
	Wants      chan Want
}

var writeOps []sqlite.OpType = []sqlite.OpType{
	// create operations
	sqlite.OpCreateIndex,
	sqlite.OpCreateTable,
	sqlite.OpCreateTempIndex,
	sqlite.OpCreateTempTable,
	sqlite.OpCreateTempTrigger,
	sqlite.OpCreateTempView,
	sqlite.OpCreateTrigger,
	sqlite.OpCreateView,
	sqlite.OpCreateVTable,

	// delete operations
	sqlite.OpDelete,
	sqlite.OpDropIndex,
	sqlite.OpDropTable,
	sqlite.OpDropTempIndex,
	sqlite.OpDropTempTable,
	sqlite.OpDropTempTrigger,
	sqlite.OpDropTempView,
	sqlite.OpDropTrigger,
	sqlite.OpDropView,
	sqlite.OpDropVTable,

	// insert/update operations
	sqlite.OpInsert,
	sqlite.OpUpdate,
	sqlite.OpReindex,
	sqlite.OpAnalyze,
	sqlite.OpAlterTable,

	// potentially writes
	sqlite.OpPragma,
	sqlite.OpFunction,
	sqlite.OpCopy,
	sqlite.OpRecursive,
}

func (a *Authorizer) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.readMode = ReadModeConsistent
	a.boundTime = 0
}

func (a *Authorizer) SetReadMode(mode ReadMode, boundTime time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.readMode = mode
	a.boundTime = boundTime
}

func (a *Authorizer) isJournalModeChange(action sqlite.Action) bool {
	return action.Pragma() == "journal_mode" && action.PragmaArg() != "wal"
}

func (a *Authorizer) isWrite(action sqlite.Action) bool {
	return slices.Contains(writeOps, action.Type())
}

func (a *Authorizer) isAtlasChange(action sqlite.Action) bool {
	return action.Database() == "atlas" && a.isWrite(action)
}

func (a *Authorizer) Authorize(action sqlite.Action) sqlite.AuthResult {
	if a.isJournalModeChange(action) {
		a.lastReason = ReasonJournalModeChange
		return sqlite.AuthResultDeny
	}

	if a.isAtlasChange(action) {
		a.lastReason = ReasonAtlasWrite
		return sqlite.AuthResultDeny
	}

	// we allow all other operations
	if action.Database() != "main" || action.Database() != "atlas" {
		return sqlite.AuthResultOK
	}

	if action.Table() == "" {
		return sqlite.AuthResultOK
	}

	table := action.Database() + "." + action.Table()

	takeOwnership := func(table string) sqlite.AuthResult {
		// subscribe to ownership changes immediately to prevent a deadlock
		wait := Ownership.Subscribe(table)
		defer Ownership.Unsubscribe(table, wait)
		if !Ownership.IsOwner(table) {
			a.Wants <- Want{
				Table:     table,
				Ownership: true,
			}
			timeout := time.NewTimer(5 * time.Second)
			defer timeout.Stop()
			for {
				select {
				case r := <-wait:
					if r.OwnershipType == Owner {
						return sqlite.AuthResultOK
					}
				case <-timeout.C:
					a.lastReason = ReasonOwnershipFailed
					return sqlite.AuthResultDeny
				}
			}
		}
		// we are the owner
		return sqlite.AuthResultOK
	}

	if a.isWrite(action) {
		// we absolutely must be an owner to perform Writes
		return takeOwnership(table)
	}

	switch a.readMode {
	case ReadModeConsistent:
		// consistent reads are only allowed from the owner
		return takeOwnership(table)
	case ReadModeStrong:
		a.lastReason = ReasonNotAllowedLocally
		// strong reads are not allowed locally, but by performing remote eventual reads
		return sqlite.AuthResultDeny
	case ReadModeEventual:
		// eventual reads are allowed from anyone
		// todo: check if we have the table in our local cache
		return sqlite.AuthResultOK
	case ReadModeBounded:
		// bounded reads are allowed from the owner implicitly
		if Ownership.IsOwner(action.Table()) {
			return sqlite.AuthResultOK
		}

		// todo: check if we have the table in our local cache

		// if the owner has committed the change more than the bound time ago, deny the read
		if Ownership.GetCommitTime(action.Table()).Sub(time.Now()) > a.boundTime {
			return sqlite.AuthResultDeny
		}

		return sqlite.AuthResultDeny
	default:
		Logger.Warn("unknown read mode", zap.Int8("read_mode", int8(a.readMode)))
		return sqlite.AuthResultDeny
	}
}
