// pgfs implements a file system that reads and writes files to Postgres
// as [Large Objects].
//
// [FS] represents a flat file system where files use UUID strings as names.
// They're meant to be written once, then used as immutable blobs afterwards.
//
// Files are stored as large objects, and tracked in a dedicated metadata table
// called "pgfs_metadata". The table can be created by calling [MigrateUp].
//
// To prevent orphaned files, the "id" column in "pgfs_metadata" can be referenced
// as a foreign key in any table. Use an "ON DELETE" constraint to prevent rows from
// being deleted before the file they reference has been removed using [FS.Remove].
//
//	CREATE TABLE user_files (
//		[...]
//		file_id UUID NOT NULL,
//		FOREIGN KEY (file_id) REFERENCES pgfs_metadata (id) ON DELETE RESTRICT,
//		[...]
//	);
//
// [Large Objects]: https://www.postgresql.org/docs/current/largeobjects.html
package pgfs

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/http"

	_ "embed"

	"github.com/google/uuid"
)

// GenerateID returns a new random UUID string.
func GenerateUUID() string {
	return uuid.New().String()
}

// BinaryType is the generic MIME type for
// binary content.
const BinaryType = "application/octet-stream"

// Tx represents a database transaction type, such as [sql.Tx].
type Tx interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
	Rollback() error
	Commit() error
}

var _ Tx = &sql.Tx{}

// ValidPath is analog to [fs.ValidPath], and checks
// if name is a valid UUID.
func ValidPath(name string) bool {
	if name == "" {
		return true
	}
	_, err := uuid.Parse(name)
	return err == nil
}

// FS implements a file system using the Large Objects API
// of Postgres.
//
// FS implements [fs.StatFS] and [fs.ReadDirFS].
type FS struct {
	conn Tx
}

// New returns a new instance of [FS] bound to
// a database transaction.
func New(conn Tx) (*FS, error) {
	return &FS{conn: conn}, nil
}

// ReadFile returns the content of the file with the
// given name.
func (fsys *FS) ReadFile(name string) ([]byte, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// ReadDir implements [fs.ReadDirFS].
//
// An error is returned if name is not an empty string.
func (fsys *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	const q = `
	  SELECT 
			id, oid, created_at,
			content_size, content_type, content_sha256
	  FROM pgfs_metadata
	  ORDER BY id ASC
	`
	rows, err := fsys.conn.Query(q)
	if err != nil {
		return nil, err
	}

	entries := make([]fs.DirEntry, 0)
	defer rows.Close()
	for rows.Next() {
		e := &entry{}
		err := rows.Scan(
			&e.id,
			&e.oid,
			&e.createdAt,
			&e.contentSize,
			&e.contentType,
			&e.contentSHA256,
		)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func (fsys *FS) rootInfo() (fs.FileInfo, error) {
	const q = `
		WITH agg AS (
			SELECT SUM(content_size) AS content_size
			FROM pgfs_metadata
		)
		SELECT 
			COALESCE(created_at, NOW()) as created_at, 
			COALESCE((SELECT content_size FROM agg), 0) as content_size 
		FROM pgfs_metadata
		ORDER BY created_at DESC
		LIMIT 1
	`
	fi := &entry{
		id:    rootUUID,
		isDir: true,
		mode:  fs.ModeDir,
	}
	err := fsys.conn.QueryRow(q).Scan(&fi.createdAt, &fi.contentSize)
	if err != nil {
		return nil, err
	}
	return fi, nil

}

// Stat returns info on the file with the given name.
//
// If name is an empty string, the returned info is on the
// root directory.
//
// The returned value implements [FileInfo].
func (fsys *FS) Stat(name string) (fs.FileInfo, error) {
	if name == "" {
		return fsys.rootInfo()
	}

	id, err := uuid.Parse(name)
	if err != nil {
		return nil, fs.ErrNotExist
	}

	const q = `
	  SELECT 
			oid, created_at,
			content_size, content_type, content_sha256
		FROM pgfs_metadata
		WHERE id = $1
	`
	row := fsys.conn.QueryRow(q, id)
	info := &entry{
		id:    id,
		isDir: false,
		mode:  fs.ModeIrregular,
	}
	err = row.Scan(
		&info.oid,
		&info.createdAt,
		&info.contentSize,
		&info.contentType,
		&info.contentSHA256,
	)
	if err == sql.ErrNoRows {
		err = fs.ErrNotExist
	}
	return info, err
}

// Open returns the file with the given name.
//
// If name is an empty string, the root directory
// is returned.
func (fsys *FS) Open(name string) (fs.File, error) {
	if name == "" {
		di, err := fsys.Stat("")
		if err != nil {
			return nil, err
		}
		return &dir{fsys: fsys, info: di.(*entry)}, nil
	}

	id, err := uuid.Parse(name)
	if err != nil {
		return nil, fs.ErrNotExist
	}

	info, fd, err := open(fsys.conn, id, invRead)
	if err != nil {
		return nil, err
	}

	f := &file{
		fd:   fd,
		fsys: fsys,
		info: info,
	}
	return f, nil
}

// Create returns a writer to a new file with the given
// name and content type. The caller must close the writer
// for the operation to complete.
//
// Name must be a valid and unique UUID. If an empty string is passed,
// a random one will be generated and used.
func (fsys *FS) Create(name, contentType string) (io.WriteCloser, error) {
	id, err := uuid.Parse(name)
	if err != nil {
		return nil, errors.New("name must be a valid UUID string")
	}

	oid, fd, err := create(fsys.conn, id)
	if err != nil {
		return nil, err
	}

	w := &writer{
		fd:          fd,
		oid:         oid,
		fsys:        fsys,
		hasher:      sha256.New(),
		id:          id,
		contentType: contentType,
	}
	return w, nil
}

// Remove deletes the file with the given name.
func (fsys *FS) Remove(name string) error {
	id, err := uuid.Parse(name)
	if err != nil {
		return fs.ErrNotExist
	}

	return remove(fsys.conn, id)
}

var (
	_ fs.StatFS    = &FS{}
	_ fs.ReadDirFS = &FS{}
)

// ServeFile serves the content of a file over HTTP.
//
// If f is a file created by this package, [http.ServeContent]
// is used after adding the appropriate headers sourced
// from its [FileInfo].
//
//	[...]
//	ETag: "{ FileInfo.ContentSHA256() }"
//	Last-Modified: "{ FileInfo.ModTime() }"
//	Content-Type: "{ FileInfo.ContentType() }"
//	Repr-Digest: "sha-256=:{ FileInfo.ContentSHA256() }:"
//	[...]
func ServeFile(w http.ResponseWriter, r *http.Request, f fs.File) {
	if handler, ok := f.(http.Handler); ok {
		handler.ServeHTTP(w, r)
		return
	}

	info, err := f.Stat()
	if err != fs.ErrNotExist {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		log.Printf("error reading file stat: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if info.IsDir() {
		log.Printf("error serving directory")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if rsc, ok := f.(io.ReadWriteSeeker); ok {
		http.ServeContent(w, r, info.Name(), info.ModTime(), rsc)
		return
	}

	w.Header().Set("Last-Modified", info.ModTime().Format(http.TimeFormat))
	if _, err := io.Copy(w, f); err != nil {
		log.Printf("error copying file to response: %v", err)
	}
}
