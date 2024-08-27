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
    	state TEXT NOT NULL,
    	data BLOB NOT NULL,
    	added_at INT NOT NULL,
    	retry_count INT NOT NULL DEFAULT 0,
    	failure_reason TEXT
    ) STRICT;
    `,
	)
	if err != nil {
		return err
	}

	return nil
}
