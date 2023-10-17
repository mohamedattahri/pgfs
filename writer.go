package pgfs

import (
	"hash"
	"io/fs"
	"math"
	"net/http"

	"github.com/google/uuid"
)

// writer writes data in a large object,
// and inserts a row in the metadata table
// when closed.
type writer struct {
	fd          int32
	oid         OID
	id          uuid.UUID
	sys         Sys
	contentType string
	size        int64
	hasher      hash.Hash
	fsys        *FS
	closed      bool
	tag         []byte // holds the first 512 bytes
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

	// Store up to 512b for [http.DetectContentType].
	if w.contentType == "" {
		if m := 512 - len(w.tag); n > 0 && m > 0 {
			i := int(math.Min(float64(n), float64(m)))
			w.tag = append(w.tag, b[:i]...)
		}
	}

	return
}

// Close implements [io.WriteCloser].
func (w *writer) Close() error {
	if w.closed {
		return fs.ErrClosed
	}

	if w.contentType == "" {
		w.contentType = http.DetectContentType(w.tag)
	}

	const q = `
	  INSERT INTO pgfs_metadata (
			oid, id, sys,
			content_size, content_type, content_sha256
		) 
		VALUES (
			$1, $2, $3,
			$4, $5, $6
		)
  `
	if _, err := w.fsys.conn.Exec(q, w.oid, w.id, w.sys, w.size, w.contentType, w.hasher.Sum(nil)); err != nil {
		return err
	}
	if err := close(w.fsys.conn, w.fd); err != nil {
		return err
	}

	w.closed = true
	return nil
}
