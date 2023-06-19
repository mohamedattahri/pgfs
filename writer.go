package pgfs

import (
	"hash"

	"github.com/google/uuid"
)

// writer writes data in a large object,
// and inserts a row in the metadata table
// when closed.
type writer struct {
	fd          int32
	oid         OID
	id          uuid.UUID
	contentType string
	written     int64
	hasher      hash.Hash
	fsys        *FS
}

func (w *writer) Write(b []byte) (n int, err error) {
	n, err = write(w.fsys.conn, w.fd, b)
	if err != nil {
		return
	}
	w.written += int64(n)
	w.hasher.Write(b[:n])
	return
}

func (w *writer) Close() error {
	const q = `
	  INSERT INTO pgfs_metadata (
			oid, id, 
			content_size, content_type, content_sha256
		) 
		VALUES (
			$1, $2,
			$3, $4, $5
		)
  `
	_, err := w.fsys.conn.Exec(q, w.oid, w.id, w.written, w.contentType, w.hasher.Sum(nil))
	if err != nil {
		return err
	}
	return close(w.fsys.conn, w.fd)
}
