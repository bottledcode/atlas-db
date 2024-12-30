package atlas

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"os"
	"strings"
	"time"
	"zombiezen.com/go/sqlite"
)

var Logger *zap.Logger

func GetCurrentSession(ctx context.Context) *sqlite.Session {
	return ctx.Value("atlas-session").(*sqlite.Session)
}

// InitializeSession creates a new session for the provided SQLite connection and attaches all tables with a replication
// level of "regional" or "global" to it. It returns the updated context with the session attached. If an error occurs
// during session creation or table attachment, it returns the original context and the error.
func InitializeSession(ctx context.Context, conn *sqlite.Conn, key string) (context.Context, error) {
	var err error
	session, err := conn.CreateSession("")
	if err != nil {
		return ctx, err
	}

	m, err := MigrationsPool.Take(ctx)
	if err != nil {
		return ctx, err
	}
	defer MigrationsPool.Put(m)

	results, err := ExecuteSQL(ctx, "select table_name from tables where replication_level in ('regional', 'global')", m, false)
	if err != nil {
		return ctx, err
	}
	for _, row := range results.Rows {
		tableName := row.GetColumn("table_name").GetString()
		if err = session.Attach(tableName); err != nil {
			return ctx, err
		}
	}

	return context.WithValue(ctx, key+"-session", session), nil
}

type ValueColumn interface {
	GetString() string
	GetInt() int64
	GetFloat() float64
	GetBool() bool
	GetBlob() []byte
	IsNull() bool
	GetTime() time.Time
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

func (u *UnknownValueColumn) GetBlob() []byte {
	panic("not a blob")
}

func (u *UnknownValueColumn) IsNull() bool {
	return false
}

func (u *UnknownValueColumn) GetTime() time.Time {
	panic("not a time")
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

type ValueColumnFloat struct {
	UnknownValueColumn
	Value float64
}

func (v *ValueColumnFloat) GetFloat() float64 {
	return v.Value
}

type ValueColumnBlob struct {
	UnknownValueColumn
}

func (v *ValueColumnBlob) GetBlob() []byte {
	panic("attempted to read blob from select, use open blob")
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
				row.Columns = append(row.Columns, &ValueColumnBlob{})
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

// WritePatchset creates a file named "patchset.txt" and writes the current SQLite session's changeset to it. It retrieves the session from the provided context and handles potential errors during file creation or changeset writing. If an error occurs during file creation or writing the changeset, it prints an error message and returns.
func WritePatchset(ctx context.Context) {
	file, err := os.Create("patchset.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	session := GetCurrentSession(ctx)

	if err := session.WriteChangeset(file); err != nil {
		fmt.Println("Error writing patchset:", err)
		return
	}
	fmt.Println("Patchset written to patchset.txt")
}

func ApplyPatchset(db *sqlite.Conn) {
	file, err := os.Open("patchset.txt")
	if err != nil {
		fmt.Println("Error opening patchset:", err)
		return
	}

	err = db.ApplyChangeset(file, nil, func(conflictType sqlite.ConflictType, iterator *sqlite.ChangesetIterator) sqlite.ConflictAction {
		fmt.Println("Conflict detected:", conflictType)
		return sqlite.ChangesetReplace
	})
	if err != nil {
		fmt.Println("Error applying patchset:", err)
	}
}
