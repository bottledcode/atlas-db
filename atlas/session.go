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

func CaptureChanges(query string, db *sqlite.Conn, output bool) {
	stmt, err := db.Prepare(query)
	if err != nil {
		fmt.Println("SQL Prepare Error:", err)
		return
	}

	for {
		hasRow, err := stmt.Step()
		if err != nil {
			fmt.Println("SQL Step Error:", err)
			return
		}
		if !hasRow {
			break
		}
		if !output {
			continue
		}
		cols := stmt.ColumnCount()
		for i := 0; i < cols; i++ {
			fmt.Printf("%s: %v\t", stmt.ColumnName(i), stmt.ColumnText(i))
		}
		fmt.Println()
	}
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
