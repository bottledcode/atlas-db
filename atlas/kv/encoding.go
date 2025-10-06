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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// KeyBuilder helps construct hierarchical keys for different data types
type KeyBuilder struct {
	isMeta           bool
	isIndex          bool
	isUncommitted    bool
	table            string
	row              string
	extra            [][]byte
	migrationTable   string
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
				builder.migrationTable = string(parts[i+1])
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
func (kb *KeyBuilder) Migration(table string, version int64) *KeyBuilder {
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
	if kb.migrationTable != "" {
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

// Value represents a typed value that can be stored in the KV store
type Value struct {
	Type     TypeCode       `json:"type"`
	Data     any            `json:"data"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// TypeCode represents the data type
type TypeCode int

const (
	TypeString TypeCode = iota + 1
	TypeInt
	TypeFloat
	TypeBool
	TypeBlob
	TypeNull
	TypeTime
	TypeDuration
	TypeJSON
)

// String returns the string representation of TypeCode
func (tc TypeCode) String() string {
	switch tc {
	case TypeString:
		return "string"
	case TypeInt:
		return "int"
	case TypeFloat:
		return "float"
	case TypeBool:
		return "bool"
	case TypeBlob:
		return "blob"
	case TypeNull:
		return "null"
	case TypeTime:
		return "time"
	case TypeDuration:
		return "duration"
	case TypeJSON:
		return "json"
	default:
		return "unknown"
	}
}

// NewStringValue creates a string value
func NewStringValue(s string) *Value {
	return &Value{Type: TypeString, Data: s}
}

// NewIntValue creates an integer value
func NewIntValue(i int64) *Value {
	return &Value{Type: TypeInt, Data: i}
}

// NewFloatValue creates a float value
func NewFloatValue(f float64) *Value {
	return &Value{Type: TypeFloat, Data: f}
}

// NewBoolValue creates a boolean value
func NewBoolValue(b bool) *Value {
	return &Value{Type: TypeBool, Data: b}
}

// NewBlobValue creates a blob value
func NewBlobValue(data []byte) *Value {
	return &Value{Type: TypeBlob, Data: data}
}

// NewNullValue creates a null value
func NewNullValue() *Value {
	return &Value{Type: TypeNull, Data: nil}
}

// NewTimeValue creates a time value
func NewTimeValue(t time.Time) *Value {
	return &Value{Type: TypeTime, Data: t.Format(time.RFC3339Nano)}
}

// NewDurationValue creates a duration value
func NewDurationValue(d time.Duration) *Value {
	return &Value{Type: TypeDuration, Data: int64(d)}
}

// NewJSONValue creates a JSON value
func NewJSONValue(data any) *Value {
	return &Value{Type: TypeJSON, Data: data}
}

// Encode serializes the value to bytes
func (v *Value) Encode() ([]byte, error) {
	return json.Marshal(v)
}

// GetString returns the value as a string
func (v *Value) GetString() string {
	switch v.Type {
	case TypeString:
		if s, ok := v.Data.(string); ok {
			return s
		}
	case TypeInt:
		if i, ok := v.Data.(float64); ok { // JSON unmarshals numbers as float64
			return strconv.FormatInt(int64(i), 10)
		}
	case TypeFloat:
		if f, ok := v.Data.(float64); ok {
			return strconv.FormatFloat(f, 'f', -1, 64)
		}
	case TypeBool:
		if b, ok := v.Data.(bool); ok {
			return strconv.FormatBool(b)
		}
	case TypeTime:
		if s, ok := v.Data.(string); ok {
			return s
		}
	case TypeNull:
		return ""
	}
	return fmt.Sprintf("%v", v.Data)
}

// GetInt returns the value as an integer
func (v *Value) GetInt() int64 {
	switch v.Type {
	case TypeInt:
		if i, ok := v.Data.(float64); ok { // JSON unmarshals numbers as float64
			return int64(i)
		}
	case TypeString:
		if s, ok := v.Data.(string); ok {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i
			}
		}
	case TypeBool:
		if b, ok := v.Data.(bool); ok {
			if b {
				return 1
			}
			return 0
		}
	case TypeDuration:
		if i, ok := v.Data.(float64); ok {
			return int64(i)
		}
	}
	return 0
}

// GetFloat returns the value as a float
func (v *Value) GetFloat() float64 {
	switch v.Type {
	case TypeFloat:
		if f, ok := v.Data.(float64); ok {
			return f
		}
	case TypeInt:
		if i, ok := v.Data.(float64); ok {
			return i
		}
	case TypeString:
		if s, ok := v.Data.(string); ok {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
	}
	return 0.0
}

// GetBool returns the value as a boolean
func (v *Value) GetBool() bool {
	switch v.Type {
	case TypeBool:
		if b, ok := v.Data.(bool); ok {
			return b
		}
	case TypeInt:
		if i, ok := v.Data.(float64); ok {
			return int64(i) != 0
		}
	case TypeString:
		if s, ok := v.Data.(string); ok {
			if b, err := strconv.ParseBool(s); err == nil {
				return b
			}
		}
	}
	return false
}

// GetBlob returns the value as bytes
func (v *Value) GetBlob() []byte {
	switch v.Type {
	case TypeBlob:
		if data, ok := v.Data.([]byte); ok {
			return data
		}
		// JSON base64 encoded
		if s, ok := v.Data.(string); ok {
			// This would need base64 decoding in real implementation
			return []byte(s)
		}
	case TypeString:
		if s, ok := v.Data.(string); ok {
			return []byte(s)
		}
	}
	return nil
}

// IsNull returns true if the value is null
func (v *Value) IsNull() bool {
	return v.Type == TypeNull
}

// GetTime returns the value as time.Time
func (v *Value) GetTime() time.Time {
	switch v.Type {
	case TypeTime:
		if s, ok := v.Data.(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return t
			}
		}
	case TypeString:
		if s, ok := v.Data.(string); ok {
			if t, err := time.Parse(time.DateTime, s); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

// GetDuration returns the value as time.Duration
func (v *Value) GetDuration() time.Duration {
	switch v.Type {
	case TypeDuration:
		if i, ok := v.Data.(float64); ok {
			return time.Duration(int64(i))
		}
	case TypeInt:
		if i, ok := v.Data.(float64); ok {
			return time.Duration(int64(i))
		}
	}
	return 0
}

// DecodeValue deserializes bytes back to a Value
func DecodeValue(data []byte) (*Value, error) {
	var v Value
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Record represents a collection of named values (equivalent to a SQL row)
type Record struct {
	Fields  map[string]*Value `json:"fields"`
	Version uint64            `json:"version"`
	Created time.Time         `json:"created"`
	Updated time.Time         `json:"updated"`
}

// NewRecord creates a new record
func NewRecord() *Record {
	now := time.Now()
	return &Record{
		Fields:  make(map[string]*Value),
		Version: 1,
		Created: now,
		Updated: now,
	}
}

// SetField sets a field value
func (r *Record) SetField(name string, value *Value) {
	r.Fields[name] = value
	r.Updated = time.Now()
	r.Version++
}

// GetField gets a field value
func (r *Record) GetField(name string) (*Value, bool) {
	value, exists := r.Fields[name]
	return value, exists
}

// Encode serializes the record to bytes
func (r *Record) Encode() ([]byte, error) {
	return json.Marshal(r)
}

// DecodeRecord deserializes bytes back to a Record
func DecodeRecord(data []byte) (*Record, error) {
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func DecodeItem(item *badger.Item) (string, *Record, error) {
	key := string(item.KeyCopy(nil))
	value, err := item.ValueCopy(nil)
	if err != nil {
		return "", nil, err
	}
	record, err := DecodeRecord(value)
	if err != nil {
		return "", nil, err
	}
	return key, record, nil
}

// ParseTableRowKey returns the given key. Key === table in atlasdb
func ParseTableRowKey(key []byte) (tableName, rowID string, valid bool) {
	keyStr := string(key)
	parts := strings.Split(keyStr, keySeparator)

	if len(parts) >= 4 && parts[0] == keyTable && parts[2] == keyRow {
		return parts[1], parts[3], true
	} else if len(parts) >= 2 && parts[0] == keyTable {
		return parts[1], "", true
	}

	return "", "", false
}
