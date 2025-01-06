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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCommandFromString(t *testing.T) {
	command := "SELECT * FROM table"
	cs := commandFromString(command)

	assert.Equal(t, command, cs.raw)

	expectedNormalized := "SELECT * FROM TABLE"
	assert.Equal(t, expectedNormalized, cs.normalized)

	expectedParts := []string{"SELECT", "*", "FROM", "TABLE"}
	assert.Equal(t, expectedParts, cs.parts)
}

func TestValidate(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	err := cs.validate(4)
	assert.NoError(t, err)

	err = cs.validate(5)
	assert.Errorf(t, err, "expected error, got nil")
}

func TestValidateExact(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	err := cs.validateExact(4)
	assert.NoError(t, err)

	err = cs.validateExact(3)
	assert.Errorf(t, err, "expected error, got nil")
}

func TestRemoveCommand(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	newCs := cs.removeCommand(2)

	expected := "FROM table"
	assert.Equal(t, expected, newCs.raw)
}

func TestRemoveButKeepSpace(t *testing.T) {
	cs := commandFromString("bind cu :name text  ")
	newCs := cs.removeCommand(4)

	expected := " "
	assert.Equal(t, expected, newCs.raw)
}

func TestSelectCommand(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	part := cs.selectCommand(2)

	expected := "FROM"
	assert.Equal(t, expected, part)
}

func TestSelectSpace(t *testing.T) {
	cs := commandFromString("bind cu :name text  ")
	part := cs.selectCommand(4)

	expected := " "
	assert.Equal(t, expected, part)
}

func TestSelectNormalizedCommand(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	part := cs.selectNormalizedCommand(2)

	expected := "FROM"
	assert.Equal(t, expected, part)
}

func TestReplaceCommand(t *testing.T) {
	cs := commandFromString("SELECT LOCAL * FROM table")
	newCs := cs.replaceCommand("SELECT LOCAL", "SELECT")

	expected := "SELECT * FROM table"
	assert.Equal(t, expected, newCs.raw)
}
