package pgfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	_ "embed" // Testing files

	_ "github.com/jackc/pgx/v5/stdlib" // Postgres driver
	"golang.org/x/exp/maps"
)

var TestDB *sql.DB

//go:embed testing/gopher.png
var TestBytes []byte

// TestBytesSHA256 is the SHA-256 of the test bytes.
var TestBytesSHA256 []byte

func init() {
	digest := sha256.Sum256(TestBytes)
	TestBytesSHA256 = digest[:sha256.Size]
}

func connect(url string) (*sql.DB, error) {
	var db *sql.DB

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("Connecting to database: %s", url)
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}

	go func() {
		var (
			interval = 2 * time.Second
			retries  int
		)
		for ctx.Err() == nil {
			if err := db.Ping(); err == nil {
				cancel()
				break
			}
			retries++
			log.Printf("(#%d) database not accessible. Retrying in %s...", retries, interval.String())
			time.Sleep(interval)
		}
	}()

	<-ctx.Done()
	if err := ctx.Err(); err != context.Canceled {
		log.Fatalf("unable to connect to database: %v", err)
	}
	return db, nil
}

func migrate(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := MigrateUp(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func reset(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := MigrateDown(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func withFS(t *testing.T, fn func(fsys *FS)) {
	tx, err := TestDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			t.Log(err)
		}
	})

	fn(New(tx))

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func createFile(t *testing.T, fsys *FS, name, contentType string, sys Sys) {
	w, err := fsys.Create(name, contentType, sys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(TestBytes); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFSStat(t *testing.T) {
	withFS(t, func(fsys *FS) {
		var (
			name        = GenerateUUID()
			contentType = "image/png"
			sys         = Sys{
				"a": "1",
				"b": "2",
				"c": "3",
			}
		)
		createFile(t, fsys, name, contentType, sys)

		info, err := fsys.Stat(name)
		if err != nil {
			t.Fatal("error getting info on created file", err)
		}

		if info.Name() != name {
			t.Error("names don't match. Wanted:", name, "Got:", info.Name())
		}
		if info.Size() != int64(len(TestBytes)) {
			t.Error("sizes don't match. Wanted:", len(TestBytes), "Got:", info.Size())
		}
		if info.ModTime().IsZero() {
			t.Error("time is zero")
		}

		fi, ok := info.(FileInfo)
		if !ok {
			t.Fatal("info.Sys is not of type *Sys")
		}

		m, ok := fi.Sys().(Sys)
		if !ok {
			t.Error("not of type Sys")
		}
		if !maps.Equal(m, sys) {
			t.Error("sys doesn't match")
		}

		if fi.ContentType() != contentType {
			t.Error("content types don't match. Wanted", contentType, "Got", fi.ContentType())
		}
		if fi.OID() == 0 {
			t.Error("OID should not be nil")
		}
		if !bytes.Equal(fi.ContentSHA256(), TestBytesSHA256) {
			t.Error("SHA256 digests don't match")
		}
	})
}

func TestFileRead(t *testing.T) {
	withFS(t, func(fsys *FS) {
		name := GenerateUUID()
		createFile(t, fsys, name, BinaryType, nil)

		f, err := fsys.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })

		b, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(b, TestBytes) {
			t.Log(string(b), string(TestBytes))
			t.Fatal("bytes don't match")
		}
	})
}

func TestFileSeek(t *testing.T) {
	withFS(t, func(fsys *FS) {
		name := GenerateUUID()
		createFile(t, fsys, name, BinaryType, nil)

		f, err := fsys.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })

		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}

		seeker, ok := f.(io.Seeker)
		if !ok {
			t.Fatal("file is not an io.Seeker")
		}

		pos, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if pos != 0 {
			t.Fatal("wrong position. Wanted 0. Got", pos)
		}

		pos, err = seeker.Seek(0, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}
		if pos != info.Size() {
			t.Fatal("wrong position. Wanted:", info.Size(), "Got:", pos)
		}

		val := int64(math.Ceil(float64(info.Size()) / 2))
		pos, err = seeker.Seek(-val, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if wanted := info.Size() - val; pos != wanted {
			t.Fatal("wrong position. Wanted:", wanted, "Got:", pos)
		}

		p, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		if wanted := info.Size() - val; int64(len(p)) != wanted {
			t.Fatal("wrong amount of data read. Wanted:", wanted, "Got:", len(p))
		}
	})
}

func TestReadFile(t *testing.T) {
	withFS(t, func(fsys *FS) {
		name := GenerateUUID()
		createFile(t, fsys, name, BinaryType, nil)

		b, err := fsys.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(b, TestBytes) {
			t.Fatal("bytes don't match")
		}
	})
}

func TestFSRemoveNotExist(t *testing.T) {
	withFS(t, func(fsys *FS) {
		err := fsys.Remove(GenerateUUID())
		if err != fs.ErrNotExist {
			t.Fatal("expected fs.ErrNotExist", err)
		}
	})
}

func TestFSReaddir(t *testing.T) {
	withFS(t, func(fsys *FS) {
		wanted := make([]string, 0)
		if result, err := fsys.ReadDir(""); err != nil {
			t.Fatal(err)
		} else {
			for _, item := range result {
				wanted = append(wanted, item.Name())
			}
		}

		const more = 100
		for i := 0; i < more; i++ {
			name := GenerateUUID()
			wanted = append(wanted, name)
			createFile(t, fsys, name, BinaryType, nil)
		}

		got, err := fsys.ReadDir("")
		if err != nil {
			t.Fatal(err)
		}

		if len(got) != len(wanted) {
			t.Fatal("number of files don't match", "Wanted", len(wanted), "Got", len(got))
		}

		// Sort by id ASC.
		sort.Strings(sort.StringSlice(wanted))

		for i, item := range got {
			if item.Name() != wanted[i] {
				t.Fatal("item", i, "don't match", "Wanted", wanted[i], "Got", item.Name())
			}
		}
	})
}

func TestFSRemove(t *testing.T) {
	withFS(t, func(fsys *FS) {
		name := GenerateUUID()
		createFile(t, fsys, name, BinaryType, nil)

		if err := fsys.Remove(name); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFSStatNotExist(t *testing.T) {
	withFS(t, func(fsys *FS) {
		_, err := fsys.Stat(GenerateUUID())
		if err != fs.ErrNotExist {
			t.Fatal("expected fs.ErrNotExist")
		}
	})
}

func TestFSCreate(t *testing.T) {
	withFS(t, func(fsys *FS) {
		name := GenerateUUID()
		contentType := "application/pdf"
		w, err := fsys.Create(name, contentType, nil)
		if err != nil {
			t.Fatal(err)
		}

		n, err := w.Write(TestBytes)
		if err != nil {
			t.Fatal(err)
		}
		if wanted := len(TestBytes); n != wanted {
			t.Fatalf("short write. Wanted: %d. Got: %d", wanted, n)
		}

		if err := w.Close(); err != nil {
			t.Fatalf("error closing writer: %v", err)
		}

		info, err := fsys.Stat(name)
		if err != nil {
			t.Fatal("error getting info on created file", err)
		}

		if info.Size() != int64(len(TestBytes)) {
			t.Fatal("sizes don't match")
		}
	})
}

func TestFSCreateFileExists(t *testing.T) {
	withFS(t, func(fsys *FS) {
		name := GenerateUUID()
		createFile(t, fsys, name, BinaryType, nil)

		_, err := fsys.Create(name, BinaryType, nil)
		if err != fs.ErrExist {
			t.Fatal("expected fs.ErrExist. Got", err)
		}
	})
}

// loopingReader is an [io.Reader]
// that loops over the same source
// of data without ever returning
// io.EOF.
//
// Useful because reading from crypto/rand
// would be too resource intensive for tests.
type loopingReader struct {
	src []byte
	cur int
}

// Read implements [io.Reader].
func (r *loopingReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		max := len(p) - n
		if (r.cur + max) > len(r.src) {
			max = len(r.src) - r.cur
		}
		n += copy(p[n:n+max], r.src[r.cur:r.cur+max])
		r.cur = (r.cur + n) % len(r.src)
	}
	return
}

// Test consists of two steps:
//
// (1) Writing a large 100Mb file into the database
// while computing its sha256 hash;
// (2) Reading it back from the database while
// computing another hash that can be compared
// with the first one.
func TestFSCreateLargeFile(t *testing.T) {
	withFS(t, func(fsys *FS) {
		var (
			name = GenerateUUID()
			h    = sha256.New()
		)

		w, err := fsys.Create(name, BinaryType, nil)
		if err != nil {
			t.Fatal(err)
		}

		mw := io.MultiWriter(h, w)
		written, err := io.Copy(mw, io.LimitReader(&loopingReader{src: TestBytes}, 100*1024<<10)) // 100MB
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		wDigest := h.Sum(nil)
		h.Reset()

		f, err := fsys.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })

		read, err := io.Copy(h, f)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		rDigest := h.Sum(nil)

		if written != read {
			t.Fatal("Bytes written", written, "Bytes read:", read)
		}

		if !bytes.Equal(wDigest, rDigest) {
			t.Fatal("checksums don't match")
		}
	})
}

func TestFSCreateWriteClosedFile(t *testing.T) {
	withFS(t, func(fsys *FS) {
		w, err := fsys.Create(GenerateUUID(), BinaryType, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(TestBytes); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		if _, err := w.Write(TestBytes); err != fs.ErrClosed {
			t.Fatal("expected fs.ErrClosed. Got:", err)
		}
		if err := w.Close(); err != fs.ErrClosed {
			t.Fatal("expected fs.ErrClosed. Got:", err)
		}
	})
}

func TestServeFileObject(t *testing.T) {
	withFS(t, func(fsys *FS) {
		name := GenerateUUID()
		createFile(t, fsys, name, "application/png", nil)

		f, err := fsys.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		ff := f.(*file)

		r := httptest.NewRequest(http.MethodGet, "https://example.com", nil)
		w := httptest.NewRecorder()
		ff.ServeHTTP(w, r)
		resp := w.Result()

		tests := map[string]string{
			"Content-Type":  ff.info.contentType,
			"Last-Modified": ff.info.createdAt.Format(http.TimeFormat),
			"Repr-Digest":   "sha-256=:" + base64.StdEncoding.EncodeToString(ff.info.contentSHA256) + ":",
			"ETag":          "\"" + hex.EncodeToString(ff.info.contentSHA256) + "\"",
		}
		for name, wanted := range tests {
			got := resp.Header.Get(name)
			if wanted != got {
				t.Error("header", name, "Wanted", wanted, "Got", got)
			}
		}
	})
}

func TestOpenRoot(t *testing.T) {
	withFS(t, func(fsys *FS) {
		for i := 0; i < 100; i++ {
			createFile(t, fsys, GenerateUUID(), BinaryType, nil)
		}

		d, err := fsys.Open("")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })

		info, err := d.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if !info.IsDir() {
			t.Error("info is not for a dir")
		}

		if info.Mode() != fs.ModeDir {
			t.Error("mode is not fs.ModeDir")
		}

		if info.ModTime().IsZero() {
			t.Error("invalid mod time")
		}

		if wanted := 100 * len(TestBytes); info.Size() <= int64(wanted) {
			t.Error("size is lower than expected", "Got", info.Size(), "Wanted >=", wanted)
		}
	})
}

func TestWalkFunc(t *testing.T) {
	withFS(t, func(fsys *FS) {
		for i := 0; i < 100; i++ {
			createFile(t, fsys, GenerateUUID(), BinaryType, nil)
		}

		seen := 0
		fs.WalkDir(fsys, "", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				t.Fatal(err)
			}
			seen++
			return nil
		})

		if seen < 100 {
			t.Fatal("saw fewer files than expected")
		}
	})
}

func TestMain(m *testing.M) {
	connURL := os.Getenv("POSTGRES_URL")
	if connURL == "" {
		log.Fatal("POSTGRES_URL env variable is missing or empty")
	}

	var err error
	TestDB, err = connect(connURL)
	if err != nil {
		log.Fatal(err)
	}
	defer TestDB.Close()

	if err := migrate(TestDB); err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	if err := reset(TestDB); err != nil {
		log.Fatal(err)
	}

	os.Exit(code)
}
