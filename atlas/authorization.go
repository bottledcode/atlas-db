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

package atlas

import (
	"go.uber.org/zap"
	"slices"
	"strings"
	"sync"
	"time"
	"zombiezen.com/go/sqlite"
)

type Authorizer struct {
	boundTime     time.Duration
	mu            sync.RWMutex
	LastTables    map[string]struct{}
	ForceReadonly bool
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

	a.boundTime = 0
	a.LastTables = make(map[string]struct{})
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
	Logger.Info("Auth", zap.Any("action", action), zap.String("table", action.Table()))

	if a.ForceReadonly && a.isWrite(action) {
		return sqlite.AuthResultDeny
	}

	if action.Table() != "" && action.Database() != "" && action.Table() != "sqlite_master" {
		name := strings.ToUpper(action.Database()) + "." + strings.ToUpper(action.Table())
		a.LastTables[name] = struct{}{}
	}

	if a.isJournalModeChange(action) || a.isAtlasChange(action) {
		return sqlite.AuthResultDeny
	}

	return sqlite.AuthResultOK
}
