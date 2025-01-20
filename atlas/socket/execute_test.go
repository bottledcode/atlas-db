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

package socket

import (
	"github.com/bottledcode/atlas-db/atlas/commands"
	"github.com/stretchr/testify/assert"
	"testing"
	"zombiezen.com/go/sqlite"
)

func TestParseExecute(t *testing.T) {
	tests := []struct {
		name    string
		input   commands.Command
		wantErr bool
	}{
		{"Valid Command", commands.CommandFromString("EXECUTE stmt1"), false},
		{"Invalid Command Length", commands.CommandFromString("EXECUTE"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseExecute(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExecute_Handle(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatalf("Failed to open SQLite connection: %v", err)
	}
	defer conn.Close()

	existing := conn.Prep("SELECT 1")
	defer func() {
		_ = existing.Finalize()
	}()

	existingQuery := &Query{
		stmt:  existing,
		query: commands.CommandFromString("SELECT 1").From(0),
	}

	passed := conn.Prep("SELECT 1")
	passedQuery := &Query{
		stmt:  passed,
		query: commands.CommandFromString("SELECT 1").From(0),
	}

	tests := []struct {
		name      string
		execute   *Execute
		socket    *Socket
		wantErr   bool
		errString string
	}{
		{
			name:    "Valid Statement",
			execute: &Execute{id: "stmt1"},
			socket: &Socket{
				activeStmts: map[string]*Query{
					"stmt1": {},
				},
				streams: []*sqlite.Stmt{},
			},
			wantErr: false,
		},
		{
			name:    "Statement Not Found",
			execute: &Execute{id: "stmt2"},
			socket: &Socket{
				activeStmts: map[string]*Query{
					"stmt1": existingQuery,
				},
				streams: []*sqlite.Stmt{},
			},
			wantErr:   true,
			errString: "statement not found",
		},
		{
			name:    "Statement Already Executing",
			execute: &Execute{id: "stmt1"},
			socket: &Socket{
				activeStmts: map[string]*Query{
					"stmt1": passedQuery,
				},
				streams: []*sqlite.Stmt{
					existing,
				},
			},
			wantErr:   true,
			errString: "statement already executing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.execute.Handle(tt.socket)
			if tt.wantErr {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.errString)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
