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

type Command interface {
	CheckMinLen(expected int) error
	CheckExactLen(expected int) error
	SelectCommand(k int) string
	SelectNormalizedCommand(k int) (string, bool)
	ReplaceCommand(original, new string) Command
	RemoveAfter(k int) Command
	Normalized() string
	Raw() string
	String() string
}

type CommandString struct {
	normalized string
	parts      []string
	raw        string
	rawParts   []string
}

func (c *CommandString) String() string {
	return c.raw
}

func (c *CommandString) Normalized() string {
	return c.normalized
}

func (c *CommandString) Raw() string {
	return c.raw
}

// CommandFromString creates a CommandString from a string,
// normalizing it while still allowing access to the raw command
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
		normalized: normalized,
		parts:      parts,
		raw:        command,
		rawParts:   rawParts,
	}
}

// CheckMinLen checks if the command has at least expected arguments
func (c *CommandString) CheckMinLen(expected int) error {
	if len(c.parts) < expected {
		return errors.New(c.Raw() + " expects " + strconv.Itoa(expected) + " arguments")
	}
	return nil
}

// CheckExactLen checks if the command has exactly expected arguments
func (c *CommandString) CheckExactLen(expected int) error {
	if len(c.parts) != expected {
		return errors.New(c.Raw() + " expects exactly " + strconv.Itoa(expected) + " arguments")
	}
	return nil
}

// replaceCommand returns the raw command with a new prefix.
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

// RemoveAfter returns a new CommandString with the last k commands removed.
func (c *CommandString) RemoveAfter(k int) Command {
	if k < 0 {
		k = k * -1
	}

	fields := c.rawParts[:len(c.rawParts)-k]
	if len(fields) == 0 {
		return EmptyCommandString
	}

	query := c.Raw()
	endpos := 0

	for _, field := range fields {
		// consume the field from the query
		endpos = strings.Index(strings.ToUpper(query), strings.ToUpper(field)) + len(field)
	}

	return CommandFromString(query[:endpos])
}

// removeCommand returns the raw command with the first n commands removed, while retaining pertinent white space.
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

// From returns a new SqlCommand with the first n commands removed.
func (c *CommandString) From(start int) *SqlCommand {
	str := removeCommand(c.Raw(), start)
	return &SqlCommand{
		CommandString: *CommandFromString(str),
	}
}

// SelectCommand returns the kth command from the raw command.
func (c *CommandString) SelectCommand(k int) string {
	return c.rawParts[k]
}

// SelectNormalizedCommand returns the kth command from the normalized command.
func (c *CommandString) SelectNormalizedCommand(k int) (part string, ok bool) {
	if k < len(c.parts) {
		if k < 0 {
			k = k * -1
			if k > len(c.parts) {
				return "", false
			}

			return c.parts[len(c.parts)-k], true
		}
		return c.parts[k], true
	}
	return "", false
}

// ReplaceCommand returns the raw command with the first occurrence of the original command replaced by the new command.
func (c *CommandString) ReplaceCommand(original, new string) Command {
	str := replaceCommand(c.Raw(), original, new)
	return CommandFromString(str)
}

// EmptyCommandString is an empty CommandString
var EmptyCommandString *CommandString = &CommandString{}

// NormalizedLen returns the number of parts in the normalized command.
func (c *CommandString) NormalizedLen() int {
	return len(c.parts)
}
