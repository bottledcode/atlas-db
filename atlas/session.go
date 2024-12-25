package atlas

import (
	"fmt"
	"os"
	"zombiezen.com/go/sqlite"
)

var session *sqlite.Session

func InitializeSession(conn *sqlite.Conn) error {
	var err error
	session, err = conn.CreateSession("")
	if err != nil {
		return err
	}
	return session.Attach("")
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

func WritePatchset() {
	file, err := os.Create("patchset.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
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
