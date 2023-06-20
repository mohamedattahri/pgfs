package pgfs

import (
	"hash"
	"io/fs"

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
	size        int64
	hasher      hash.Hash
	fsys        *FS
	closed      bool
}

// Write implements [io.WriteCloser].
func (w *writer) Write(b []byte) (n int, err error) {
	if w.closed {
		err = fs.ErrClosed
		return
	}

	n, err = write(w.fsys.conn, w.fd, b)
	w.size += int64(n)
	w.hasher.Write(b[:n])
	return
}

// Close implements [io.WriteCloser].
func (w *writer) Close() error {
	if w.closed {
		return fs.ErrClosed
	}

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
	if _, err := w.fsys.conn.Exec(q, w.oid, w.id, w.size, w.contentType, w.hasher.Sum(nil)); err != nil {
		return err
	}
	if err := close(w.fsys.conn, w.fd); err != nil {
		return err
	}

	w.closed = true
	return nil
}
