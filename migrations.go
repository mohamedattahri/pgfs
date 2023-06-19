package pgfs

// Table is the name of the metadata table.
// that is created when [Migrate] is called.
const Table = "pgfs_metadata"

// Up is the SQL query executed by [MigrateUp].
const Up = `
	CREATE EXTENSION IF NOT EXISTS lo;
	CREATE TABLE IF NOT EXISTS pgfs_metadata (
		id UUID NOT NULL PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
		oid OID NOT NULL UNIQUE,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		opened_at TIMESTAMP NOT NULL DEFAULT NOW(),
		content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
		content_size BIGINT NOT NULL,
		content_sha256 BYTEA NOT NULL
	);
`

// Down is the SQL query executed by [MigrateDown].
const Down = "DROP TABLE pgfs_metadata;"

// MigrateUp upgrades the database schema for
// to store files using [FS].
//
// Calling MigrateUp multiple times has no effect.
func MigrateUp(conn Tx) error {
	_, err := conn.Exec(Up)
	return err
}

// MigrateDown undoes the database schema
// changes made by [MigrateUp].
func MigrateDown(conn Tx) error {
	_, err := conn.Exec(Up)
	return err
}
