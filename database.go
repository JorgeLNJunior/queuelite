package queuelite

import (
	"context"
	"database/sql"
)

func createTables(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(
		ctx,
		`
    CREATE TABLE IF NOT EXISTS queuelite_job (
      id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      data BLOB NOT NULL,
    	added_at INT NOT NULL,
    	error_reason TEXT
    ) STRICT, WITHOUT ROWID;
    `,
	)
	if err != nil {
		return err
	}

	return nil
}
