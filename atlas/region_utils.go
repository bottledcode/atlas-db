package atlas

import (
	"context"
	"zombiezen.com/go/sqlite"
)

// GetOrAddRegion If the region does not exist, it is created.
func GetOrAddRegion(ctx context.Context, conn *sqlite.Conn, name string) (string, error) {
	results, err := ExecuteSQL(ctx, "select * from regions where name = :name", conn, false, Param{
		Name:  "name",
		Value: name,
	})
	if err != nil {
		return name, err
	}

	if len(results.Rows) > 0 {
		return results.GetIndex(0).GetColumn("name").GetString(), nil
	}

	_, err = ExecuteSQL(ctx, "insert into regions (name) values (:name)", conn, false, Param{
		Name:  "name",
		Value: name,
	})
	if err != nil {
		return name, err
	}

	return name, nil
}
