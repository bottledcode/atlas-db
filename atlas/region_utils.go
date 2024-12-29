package atlas

import (
	"context"
	"zombiezen.com/go/sqlite"
)

// GetOrAddRegion returns the ID of the region with the given name. If the region does not exist, it is created.
func GetOrAddRegion(ctx context.Context, conn *sqlite.Conn, name string) (int64, error) {
	results, err := ExecuteSQL(ctx, "select * from regions where name = :name", conn, false, Param{
		Name:  "name",
		Value: name,
	})
	if err != nil {
		return 0, err
	}

	if len(results.Rows) > 0 {
		return results.GetIndex(0).GetColumn("id").GetInt(), nil
	}

	_, err = ExecuteSQL(ctx, "insert into regions (name) values (:name)", conn, false, Param{
		Name:  "name",
		Value: name,
	})
	if err != nil {
		return 0, err
	}

	return conn.LastInsertRowID(), nil
}

func GetRegionIdFromName(ctx context.Context, conn *sqlite.Conn, name string) (int64, error) {
	results, err := ExecuteSQL(ctx, "select * from regions where name = :name", conn, false, Param{
		Name:  "name",
		Value: name,
	})
	if err != nil {
		return 0, err
	}

	if len(results.Rows) == 0 {
		return 0, nil
	}

	return results.GetIndex(0).GetColumn("id").GetInt(), nil
}

func GetRegionNameFromId(ctx context.Context, conn *sqlite.Conn, id int64) (string, error) {
	results, err := ExecuteSQL(ctx, "select * from regions where id = :id", conn, false, Param{
		Name:  "id",
		Value: id,
	})
	if err != nil {
		return "", err
	}

	if len(results.Rows) == 0 {
		return "", nil
	}

	return results.GetIndex(0).GetColumn("name").GetString(), nil
}
