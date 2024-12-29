package atlas

import (
	"golang.org/x/net/context"
)

func maybeWatchTable(ctx context.Context, query *commandString) error {
	if query.selectNormalizedCommand(0) != "CREATE" {
		return nil
	}

	var tableName string

	switch query.selectNormalizedCommand(1) {
	case "TABLE":
		tableName = query.selectNormalizedCommand(2)
	case "REGIONAL":
		tableName = query.selectNormalizedCommand(3)
	}

	if tableName == "" {
		// this is a local table

		return nil
	}

	// watch the table
	session := GetCurrentSession(ctx)
	err := session.Attach(tableName)
	if err != nil {
		return err
	}

	return nil
}

// isQueryReadOnly returns whether a query is read-only or not
func isQueryReadOnly(query *commandString) bool {
	switch query.selectCommand(0) {
	case "ALTER":
		return false
	case "CREATE":
		return false
	case "DELETE":
		return false
	case "DROP":
		return false
	case "INSERT":
		return false
	case "REINDEX":
		return false
	case "REPLACE":
		return false
	case "UPDATE":
		return false
	case "VACUUM":
		return false
	case "PRAGMA":
		return false
	}

	return true
}

func nonAllowedQuery(query *commandString) bool {
	switch query.selectCommand(0) {
	case "BEGIN":
		return true
	case "COMMIT":
		return true
	case "ROLLBACK":
		return true
	case "SAVEPOINT":
		return true
	case "RELEASE":
		return true
	case "DETACH":
		return true
	case "ATTACH":
		return true
	case "PRAGMA":
		return true
	}

	return false
}

func isQueryChangeSchema(query *commandString) bool {
	switch query.selectCommand(0) {
	case "ALTER":
		return true
	case "CREATE":
		return true
	case "DROP":
		return true
	}

	return false
}
