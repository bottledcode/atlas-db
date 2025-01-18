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
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/commands"
	"zombiezen.com/go/sqlite"
)

func TestParsePrepare(t *testing.T) {
	tests := []struct {
		name    string
		input   *commands.CommandString
		wantErr bool
	}{
		{"Valid Command", commands.CommandFromString("PREPARE stmt1 SELECT * FROM Users"), false},
		{"Invalid Command Length", commands.CommandFromString("PREPARE stmt1"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePrepare(tt.input)
			if tt.wantErr {
				assert.Errorf(t, err, "PREPARE stmt1 expects 3 arguments")
			}
		})
	}
}

func TestPrepare_Handle(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatalf("Failed to open SQLite connection: %v", err)
	}
	defer conn.Close()

	s := &Socket{
		sql:         conn,
		activeStmts: make(map[string]*sqlite.Stmt),
	}

	tests := []struct {
		name    string
		prepare *Prepare
		wantErr bool
	}{
		{"Valid Statement", &Prepare{query: commands.CommandFromString("PREPARE SELECT 1").From(1), id: "stmt1"}, false},
		{"Duplicate Statement", &Prepare{query: commands.CommandFromString("PREPARE SELECT 1").From(1), id: "stmt1"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.prepare.Handle(s)
			if tt.wantErr {
				assert.ErrorIs(t, err, FatalErr)
			}
		})
	}
}
