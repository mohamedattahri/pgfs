package pgfs_test

import (
	"database/sql"
	"io"
	"log"

	"mohamed.attahri.com/pgfs"
)

var db *sql.DB

func ExampleFS_Create() {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	fsys, err := pgfs.New(tx)
	if err != nil {
		log.Fatal(err)
	}

	w, err := fsys.Create(pgfs.GenerateUUID(), "text/plain")
	if _, err := io.WriteString(w, "Bonjour!"); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
