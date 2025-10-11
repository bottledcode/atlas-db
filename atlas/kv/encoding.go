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

package kv

import (
	"bytes"
	"strconv"
	"strings"
)

// KeyBuilder helps construct hierarchical keys for different data types
type KeyBuilder struct {
	isMeta           bool
	isIndex          bool
	isUncommitted    bool
	table            string
	row              string
	extra            [][]byte
	migrationTable   []byte
	migrationVersion int64
	tableVersion     int64
	node             int64
}

// NewKeyBuilder creates a new key builder
func NewKeyBuilder() *KeyBuilder {
	return &KeyBuilder{
		isMeta: false,
		table:  "",
		row:    "",
		extra:  [][]byte{},
	}
}

const (
	keyMeta         = "m"
	keySeparator    = ":"
	keyIndex        = "i"
	keyUncommitted  = "u"
	keyTable        = "t"
	keyRow          = "r"
	keyMigration    = "m"
	keyVersion      = "v"
	keyNode         = "n"
	keyTableVersion = "tv"
)

func NewKeyBuilderFromBytes(data []byte) *KeyBuilder {
	parts := bytes.Split(data, []byte(keySeparator))
	builder := NewKeyBuilder()
	for i := 0; i < len(parts); i++ {
		if i == 0 && string(parts[i]) == keyMeta {
			builder.isMeta = true
			continue
		}
		if (i == 0 || i == 1) && string(parts[i]) == keyIndex {
			builder.isIndex = true
			continue
		}
		if string(parts[i]) == keyUncommitted {
			builder.isUncommitted = true
			continue
		}
		if len(parts) >= i+1 {
			if string(parts[i]) == keyTable {
				builder.table = string(parts[i+1])
				i += 1
				continue
			}
			if string(parts[i]) == keyRow {
				builder.row = string(parts[i+1])
				i += 1
				continue
			}
			if string(parts[i]) == keyMigration {
				builder.migrationTable = parts[i+1]
				i += 1
				continue
			}
			if string(parts[i]) == keyVersion {
				var err error
				builder.migrationVersion, err = strconv.ParseInt(string(parts[i+1]), 10, 64)
				if err != nil {
					builder.migrationVersion = -1
				}
				i += 1
				continue
			}
			if string(parts[i]) == keyNode {
				var err error
				builder.node, err = strconv.ParseInt(string(parts[i+1]), 10, 64)
				if err != nil {
					panic("invalid node id")
				}
				i += 1
				continue
			}
			if string(parts[i]) == keyTableVersion {
				var err error
				builder.tableVersion, err = strconv.ParseInt(string(parts[i+1]), 10, 64)
				if err != nil {
					panic("invalid table version")
				}
			}
		}
		builder.extra = append(builder.extra, parts[i])
	}

	return builder
}

func (kb *KeyBuilder) GetTable() string {
	return kb.table
}

func (kb *KeyBuilder) GetRow() string {
	return kb.row
}

// Table adds a table namespace to the key
func (kb *KeyBuilder) Table(tableName string) *KeyBuilder {
	kb.table = tableName
	return kb
}

// Row adds a row identifier to the key
func (kb *KeyBuilder) Row(rowID string) *KeyBuilder {
	kb.row = rowID
	return kb
}

// Meta adds metadata namespace to the key
func (kb *KeyBuilder) Meta() *KeyBuilder {
	kb.isMeta = true
	return kb
}

func (kb *KeyBuilder) Index() *KeyBuilder {
	kb.isIndex = true
	return kb
}

func (kb *KeyBuilder) Uncommitted() *KeyBuilder {
	kb.isUncommitted = true
	return kb
}

// Migration Pass 0 to version to omit, -1 to include the version prefix, or a version to include
func (kb *KeyBuilder) Migration(table []byte, version int64) *KeyBuilder {
	kb.isMeta = true
	kb.migrationTable = table
	kb.migrationVersion = version
	return kb
}

func (kb *KeyBuilder) TableVersion(version int64) *KeyBuilder {
	kb.tableVersion = version
	return kb
}

func (kb *KeyBuilder) Node(node int64) *KeyBuilder {
	kb.node = node
	return kb
}

// Append adds a custom part to the key
func (kb *KeyBuilder) Append(part string) *KeyBuilder {
	kb.extra = append(kb.extra, []byte(part))
	return kb
}

func (kb *KeyBuilder) AppendBytes(part []byte) *KeyBuilder {
	kb.extra = append(kb.extra, part)
	return kb
}

// Build constructs the final key as bytes
func (kb *KeyBuilder) Build() []byte {
	parts := make([][]byte, 0)
	if kb.isMeta {
		parts = append(parts, []byte(keyMeta))
	}
	if kb.isIndex {
		parts = append(parts, []byte(keyIndex))
	}
	if kb.table != "" {
		parts = append(parts, []byte(keyTable), []byte(kb.table))
	}
	if kb.row != "" {
		parts = append(parts, []byte(keyRow), []byte(kb.row))
	}
	if kb.migrationTable != nil {
		parts = append(parts, []byte(keyMigration), []byte(kb.migrationTable))
		if kb.isUncommitted {
			parts = append(parts, []byte(keyUncommitted))
		}
	}
	if kb.migrationVersion > 0 {
		parts = append(parts, []byte(keyVersion), []byte(strconv.FormatInt(kb.migrationVersion, 10)))
	}
	if kb.migrationVersion < 0 {
		parts = append(parts, []byte(keyVersion))
	}
	if kb.node > 0 {
		parts = append(parts, []byte(keyNode), []byte(strconv.FormatInt(kb.node, 10)))
	}
	if kb.tableVersion > 0 {
		parts = append(parts, []byte(keyTableVersion), []byte(strconv.FormatInt(kb.tableVersion, 10)))
	}
	if len(kb.extra) > 0 {
		parts = append(parts, kb.extra...)
	}
	return bytes.Join(parts, []byte(keySeparator))
}

// String returns the key as a string (for debugging)
func (kb *KeyBuilder) String() string {
	return string(kb.Build())
}

func (kb *KeyBuilder) DottedKey() string {
	parts := make([]string, 0)
	if kb.table != "" {
		parts = append(parts, kb.table)
	}
	if kb.row != "" {
		parts = append(parts, kb.row)
	}
	if len(kb.extra) > 0 {
		for _, part := range kb.extra {
			parts = append(parts, string(part))
		}
	}
	return strings.Join(parts, ".")
}

// Clone creates a copy of the KeyBuilder
func (kb *KeyBuilder) Clone() *KeyBuilder {
	return &KeyBuilder{
		isMeta: kb.isMeta,
		table:  kb.table,
		row:    kb.row,
		extra:  kb.extra,
	}
}

// TableName attempts to extract the table name from the builder.
// It returns the first segment that follows a "table" prefix, if present.
func (kb *KeyBuilder) TableName() (string, bool) {
	return kb.table, kb.table != ""
}

// FromDottedKey constructs a KeyBuilder from a logical dotted key of the form
// "table.row.extra" -> "table:<TABLE>:row:<ROW>:EXTRA".
// Additional segments after the first two are re-joined with '.' and appended as a single part.
func FromDottedKey(key string) *KeyBuilder {
	builder := NewKeyBuilder()
	parts := strings.Split(key, ".")
	switch len(parts) {
	case 0:
		return builder
	case 1:
		return builder.Table(parts[0])
	case 2:
		return builder.Table(parts[0]).Row(parts[1])
	default:
		return builder.Table(parts[0]).Row(parts[1]).Append(strings.Join(parts[2:], "."))
	}
}
