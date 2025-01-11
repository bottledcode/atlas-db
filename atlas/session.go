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
	"context"
	"fmt"
	"go.uber.org/zap"
	"strings"
	"time"
	"zombiezen.com/go/sqlite"
)

var Logger *zap.Logger

// InitializeSession creates a new session for the provided SQLite connection and attaches all tables with a replication
// level of "regional" or "global" to it. It returns the updated context with the session attached. If an error occurs
// during session creation or table attachment, it returns the original context and the error.
func InitializeSession(ctx context.Context, conn *sqlite.Conn) (*sqlite.Session, error) {
	var err error
	session, err := conn.CreateSession("")
	if err != nil {
		return nil, err
	}

	m, err := MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer MigrationsPool.Put(m)

	results, err := ExecuteSQL(ctx, "select name from tables where replication_level in ('regional', 'global')", m, false)
	if err != nil {
		return nil, err
	}
	for _, row := range results.Rows {
		tableName := row.GetColumn("name").GetString()
		if err = session.Attach(tableName); err != nil {
			session.Delete()
			return nil, err
		}
	}

	return session, nil
}

type ValueColumn interface {
	GetString() string
	GetInt() int64
	GetFloat() float64
	GetBool() bool
	GetBlob() *[]byte
	IsNull() bool
	GetTime() time.Time
	GetDuration() time.Duration
}

type UnknownValueColumn struct {
}

func (u *UnknownValueColumn) GetString() string {
	panic("not a string")
}

func (u *UnknownValueColumn) GetInt() int64 {
	panic("not an int")
}

func (u *UnknownValueColumn) GetFloat() float64 {
	panic("not a float")
}

func (u *UnknownValueColumn) GetBool() bool {
	panic("not a boolean")
}

func (u *UnknownValueColumn) GetBlob() *[]byte {
	panic("not a blob")
}

func (u *UnknownValueColumn) IsNull() bool {
	return false
}

func (u *UnknownValueColumn) GetTime() time.Time {
	panic("not a time")
}

func (u *UnknownValueColumn) GetDuration() time.Duration {
	panic("not a duration")
}

type ValueColumnString struct {
	UnknownValueColumn
	Value string
}

func (v *ValueColumnString) GetString() string {
	return v.Value
}

func (v *ValueColumnString) GetTime() time.Time {
	t, err := time.Parse(time.DateTime, v.Value)
	if err != nil {
		Logger.Error("error parsing time", zap.Error(err))
	}
	return t
}

type ValueColumnInt struct {
	UnknownValueColumn
	Value int64
}

func (v *ValueColumnInt) GetInt() int64 {
	return v.Value
}

func (v *ValueColumnInt) GetBool() bool {
	return v.Value != 0
}

func (v *ValueColumnInt) GetString() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v *ValueColumnInt) GetDuration() time.Duration {
	return time.Duration(v.Value)
}

type ValueColumnFloat struct {
	UnknownValueColumn
	Value float64
}

func (v *ValueColumnFloat) GetFloat() float64 {
	return v.Value
}

type ValueColumnBlob struct {
	UnknownValueColumn
	Value *[]byte
}

func (v *ValueColumnBlob) GetBlob() *[]byte {
	return v.Value
}

type ValueColumnNull struct {
	UnknownValueColumn
}

func (v *ValueColumnNull) IsNull() bool {
	return true
}

type Row struct {
	Id      int
	Columns []ValueColumn
	headers *map[string]int
}

func (r *Row) GetColumn(name string) ValueColumn {
	if idx, ok := (*r.headers)[name]; ok {
		return r.Columns[idx]
	}
	return &UnknownValueColumn{}
}

type Rows struct {
	Rows    []Row
	Headers map[string]int
}

func (r *Rows) GetIndex(idx int) *Row {
	if idx < 0 || idx >= len(r.Rows) {
		return nil
	}
	return &r.Rows[idx]
}

func (r *Rows) Empty() bool {
	return len(r.Rows) == 0
}

func (r *Rows) NonSingle() bool {
	return len(r.Rows) > 1
}

// Each row is converted to a Row struct with corresponding ValueColumn implementations.
func CaptureChanges(query string, db *sqlite.Conn, output bool, params ...Param) (*Rows, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}

	for _, param := range params {
		if !strings.HasPrefix(param.Name, ":") {
			param.Name = ":" + param.Name
		}
		if v, ok := param.Value.(string); ok {
			stmt.SetText(param.Name, v)
		} else if v, ok := param.Value.(*string); ok {
			if v == nil {
				stmt.SetNull(param.Name)
			} else {
				stmt.SetText(param.Name, *v)
			}
		} else if v, ok := param.Value.(int); ok {
			stmt.SetInt64(param.Name, int64(v))
		} else if v, ok := param.Value.(int64); ok {
			stmt.SetInt64(param.Name, v)
		} else if v, ok := param.Value.(uint); ok {
			stmt.SetInt64(param.Name, int64(v))
		} else if v, ok := param.Value.(float64); ok {
			stmt.SetFloat(param.Name, v)
		} else if v, ok := param.Value.([]byte); ok {
			stmt.SetBytes(param.Name, v)
		} else if v, ok := param.Value.(*[]byte); ok {
			if v == nil {
				stmt.SetNull(param.Name)
			} else {
				stmt.SetBytes(param.Name, *v)
			}
		} else if v, ok := param.Value.(bool); ok {
			stmt.SetBool(param.Name, v)
		} else if param.Value == nil {
			stmt.SetNull(param.Name)
		} else if v, ok := param.Value.(time.Time); ok {
			stmt.SetText(param.Name, v.Format(time.RFC3339))
		} else if v, ok := param.Value.(*int64); ok {
			if v == nil {
				stmt.SetNull(param.Name)
			} else {
				stmt.SetInt64(param.Name, *v)
			}
		} else if v, ok := param.Value.(time.Duration); ok {
			stmt.SetInt64(param.Name, int64(v))
		} else {
			return nil, fmt.Errorf("unsupported parameter type: %T", param.Value)
		}
	}

	rows := &Rows{
		Rows:    make([]Row, 0),
		Headers: make(map[string]int),
	}

	for i := 0; i < stmt.ColumnCount(); i++ {
		rows.Headers[stmt.ColumnName(i)] = i
	}

	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return rows, err
		}
		if !hasRow {
			break
		}
		row := Row{
			Columns: make([]ValueColumn, 0),
			headers: &rows.Headers,
		}
		cols := stmt.ColumnCount()
		for i := 0; i < cols; i++ {
			switch stmt.ColumnType(i) {
			case sqlite.TypeText:
				row.Columns = append(row.Columns, &ValueColumnString{
					Value: stmt.ColumnText(i),
				})
			case sqlite.TypeInteger:
				row.Columns = append(row.Columns, &ValueColumnInt{
					Value: stmt.ColumnInt64(i),
				})
			case sqlite.TypeFloat:
				row.Columns = append(row.Columns, &ValueColumnFloat{
					Value: stmt.ColumnFloat(i),
				})
			case sqlite.TypeBlob:
				data := make([]byte, stmt.ColumnLen(i))
				total := 0
				for {
					read := stmt.ColumnBytes(i, data)
					total += read
					if total >= len(data) {
						break
					}
				}
				row.Columns = append(row.Columns, &ValueColumnBlob{
					Value: &data,
				})
			case sqlite.TypeNull:
				row.Columns = append(row.Columns, &ValueColumnNull{})
			}

			if output {
				fmt.Printf("%s: %v\t", stmt.ColumnName(i), stmt.ColumnText(i))
				fmt.Println()
			}
		}
		rows.Rows = append(rows.Rows, row)
	}

	return rows, nil
}
