package atlas

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
