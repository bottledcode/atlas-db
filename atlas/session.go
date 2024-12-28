package atlas

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"os"
	"zombiezen.com/go/sqlite"
)

var Logger *zap.Logger

func GetCurrentSession(ctx context.Context) *sqlite.Session {
	return ctx.Value("atlas-session").(*sqlite.Session)
}

func InitializeSession(ctx context.Context, conn *sqlite.Conn) (context.Context, error) {
	var err error
	session, err := conn.CreateSession("")
	if err != nil {
		return ctx, err
	}
	sess := session.Attach("")
	return context.WithValue(ctx, "atlas-session", sess), nil
}

type ValueColumn interface {
	GetString() string
	GetInt() int
	GetFloat() float64
	GetBool() bool
	GetBlob() []byte
	IsNull() bool
}

type UnknownValueColumn struct {
}

func (u *UnknownValueColumn) GetString() string {
	panic("not a string")
}

func (u *UnknownValueColumn) GetInt() int {
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

type ValueColumnString struct {
	UnknownValueColumn
	Value string
}

func (v *ValueColumnString) GetString() string {
	return v.Value
}

type ValueColumnInt struct {
	UnknownValueColumn
	Value int
}

func (v *ValueColumnInt) GetInt() int {
	return v.Value
}

func (v *ValueColumnInt) GetBool() bool {
	return v.Value != 0
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

type Rows struct {
	Rows    []Row
	Headers map[string]int
}

func CaptureChanges(query string, db *sqlite.Conn, output bool) (*Rows, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
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
			fmt.Println("SQL Step Error:", err)
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
					Value: stmt.ColumnInt(i),
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
			rows.Rows = append(rows.Rows, row)
		}
	}

	return rows, nil
}

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
