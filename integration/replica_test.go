package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pierrec/lz4/v4"
)

func TestReplica_Sync(t *testing.T) {
	db, sqldb := MustOpenDBs(t)
	defer MustCloseDBs(t, db, sqldb)

	// Execute a query to force a write to the WAL.
	if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	}

	// Issue initial database sync to setup generation.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position.
	dpos := db.Pos()

	c := file.NewReplicaClient(t.TempDir())
	r := litestream.NewReplica(db, "")
	c.Replica, r.Client = r, c

	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify client generation matches database.
	generations, err := c.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if got, want := len(generations), 1; got != want {
		t.Fatalf("len(generations)=%v, want %v", got, want)
	} else if got, want := generations[0], dpos.Generation; got != want {
		t.Fatalf("generations[0]=%v, want %v", got, want)
	}

	// Verify WAL matches replica WAL.
	if b0, err := os.ReadFile(db.Path() + "-wal"); err != nil {
		t.Fatal(err)
	} else if r0, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: generations[0], Index: 0, Offset: 0}); err != nil {
		t.Fatal(err)
	} else if b1, err := io.ReadAll(lz4.NewReader(r0)); err != nil {
		t.Fatal(err)
	} else if err := r0.Close(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b0, b1) {
		t.Fatalf("wal mismatch: len(%d), len(%d)", len(b0), len(b1))
	}
}

func TestReplica_Snapshot(t *testing.T) {
	db, sqldb := MustOpenDBs(t)
	defer MustCloseDBs(t, db, sqldb)

	c := file.NewReplicaClient(t.TempDir())
	r := litestream.NewReplica(db, "")
	r.Client = c

	// Execute a query to force a write to the WAL.
	if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	} else if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position & snapshot.
	pos0 := db.Pos()
	if info, err := r.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := info.Pos(), pos0.Truncate(); got != want {
		t.Fatalf("pos=%s, want %s", got, want)
	}

	// Sync database and then replica.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	} else if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Execute a query to force a write to the WAL & truncate to start new index.
	if _, err := sqldb.Exec(`INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
		t.Fatal(err)
	} else if err := db.Checkpoint(context.Background(), litestream.CheckpointModeTruncate); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position & snapshot.
	pos1 := db.Pos()
	if info, err := r.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := info.Pos(), pos1.Truncate(); got != want {
		t.Fatalf("pos=%v, want %v", got, want)
	}

	// Verify two snapshots exist.
	if infos, err := r.Snapshots(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := len(infos), 2; got != want {
		t.Fatalf("len=%v, want %v", got, want)
	} else if got, want := infos[0].Pos(), pos0.Truncate(); got != want {
		t.Fatalf("info[0]=%s, want %s", got, want)
	} else if got, want := infos[1].Pos(), pos1.Truncate(); got != want {
		t.Fatalf("info[1]=%s, want %s", got, want)
	}
}

// MustOpenDBs returns a new instance of a DB & associated SQL DB.
func MustOpenDBs(tb testing.TB) (*litestream.DB, *sql.DB) {
	tb.Helper()
	db := MustOpenDB(tb)
	return db, MustOpenSQLDB(tb, db.Path())
}

// MustCloseDBs closes db & sqldb and removes the parent directory.
func MustCloseDBs(tb testing.TB, db *litestream.DB, sqldb *sql.DB) {
	tb.Helper()
	MustCloseDB(tb, db)
	MustCloseSQLDB(tb, sqldb)
}

// MustOpenDB returns a new instance of a DB.
func MustOpenDB(tb testing.TB) *litestream.DB {
	dir := tb.TempDir()
	return MustOpenDBAt(tb, filepath.Join(dir, "db"))
}

// MustOpenDBAt returns a new instance of a DB for a given path.
func MustOpenDBAt(tb testing.TB, path string) *litestream.DB {
	tb.Helper()
	db := litestream.NewDB(path)
	db.MonitorInterval = 0 // disable background goroutine
	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}
	return db
}

// MustCloseDB closes db and removes its parent directory.
func MustCloseDB(tb testing.TB, db *litestream.DB) {
	tb.Helper()
	if err := db.Close(); err != nil && !strings.Contains(err.Error(), `database is closed`) {
		tb.Fatal(err)
	} else if err := os.RemoveAll(filepath.Dir(db.Path())); err != nil {
		tb.Fatal(err)
	}
}

// MustOpenSQLDB returns a database/sql DB.
func MustOpenSQLDB(tb testing.TB, path string) *sql.DB {
	tb.Helper()
	d, err := sql.Open("sqlite3", path)
	if err != nil {
		tb.Fatal(err)
	} else if _, err := d.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		tb.Fatal(err)
	}
	return d
}

// MustCloseSQLDB closes a database/sql DB.
func MustCloseSQLDB(tb testing.TB, d *sql.DB) {
	tb.Helper()
	if err := d.Close(); err != nil {
		tb.Fatal(err)
	}
}
