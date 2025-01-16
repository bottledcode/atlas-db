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

package commands

import (
	"errors"
	"strconv"
	"strings"
)

type CommandString struct {
	Normalized string
	parts      []string
	Raw        string
	rawParts   []string
}

func CommandFromString(command string) *CommandString {
	normalized := strings.ToUpper(command)
	parts := strings.Fields(normalized)
	normalized = strings.Join(parts, " ")
	rawParts := strings.Fields(command)

	// count whitespace at the end of string
	whitespace := 0
	for i := len(command) - 1; i >= 0; i-- {
		if command[i] == ' ' {
			whitespace++
		} else {
			break
		}
	}
	if whitespace >= 2 {
		parts = append(parts, strings.Repeat(" ", whitespace-1))
		rawParts = append(rawParts, strings.Repeat(" ", whitespace-1))
	}

	return &CommandString{
		Normalized: normalized,
		parts:      parts,
		Raw:        command,
		rawParts:   rawParts,
	}
}

func (c *CommandString) CheckMinLen(expected int) error {
	if len(c.parts) < expected {
		return errors.New(c.Raw + " expects " + strconv.Itoa(expected) + " arguments")
	}
	return nil
}

func (c *CommandString) CheckExactLen(expected int) error {
	if len(c.parts) != expected {
		return errors.New(c.Raw + " expects exactly " + strconv.Itoa(expected) + " arguments")
	}
	return nil
}

// replaceCommand replaces command in query with newPrefix.
func replaceCommand(query, command, newPrefix string) string {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return query
	}

	for _, field := range fields {
		// consume the field from the query
		endpos := strings.Index(strings.ToUpper(query), strings.ToUpper(field)) + len(field)
		query = query[endpos:]
	}

	return newPrefix + query
}

func removeCommand(query string, num int) string {
	fields := strings.Fields(query)
	// count whitespace at the end of string
	whitespace := 0
	for i := len(query) - 1; i >= 0; i-- {
		if query[i] == ' ' {
			whitespace++
		} else {
			break
		}
	}
	if whitespace >= 2 {
		fields = append(fields, strings.Repeat(" ", whitespace-1))
	}

	for i := 0; i < num; i++ {
		endpos := strings.Index(query, fields[i]) + len(fields[i])
		query = query[endpos:]
	}

	return query[1:]
}

func (c *CommandString) From(start int) *SqlCommand {
	str := removeCommand(c.Raw, start)
	return &SqlCommand{
		CommandString: *CommandFromString(str),
	}
}

func (c *CommandString) SelectCommand(k int) string {
	return c.rawParts[k]
}

func (c *CommandString) SelectNormalizedCommand(k int) (part string, ok bool) {
	if k < len(c.parts) {
		return c.parts[k], true
	}
	return "", false
}

func (c *CommandString) ReplaceCommand(original, new string) *CommandString {
	str := replaceCommand(c.Raw, original, new)
	return CommandFromString(str)
}

var EmptyCommandString *CommandString = &CommandString{}

func (c *CommandString) NormalizedLen() int {
	return len(c.parts)
}
