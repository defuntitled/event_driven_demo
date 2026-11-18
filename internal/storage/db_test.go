package storage

import (
	"path/filepath"
	"testing"
)

func TestMigrationReopen(t *testing.T) {
	t.Parallel()
	for _, role := range []string{"orders", "payments", "reservations", "demo-web"} {
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "test.db")
			for range 2 {
				db, err := Open(t.Context(), path, role)
				if err != nil {
					t.Fatal(err)
				}
				var n int
				if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM schema_migrations").Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n < 1 {
					t.Fatal("no migration applied")
				}
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestStorageConstraints(t *testing.T) {
	t.Parallel()
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "test.db"), "reservations")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs(run_id,generation,scenario,initial_seats,created_at_ms) VALUES ('run1',1,'happy_path',1,0)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), "INSERT INTO inventory(run_id,concert_id,capacity,available) VALUES ('run1','concert-1',1,1)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), "UPDATE inventory SET available=-1")
	if err == nil {
		t.Fatal("negative inventory accepted")
	}
	_, err = db.ExecContext(t.Context(), "UPDATE inventory SET available=2")
	if err == nil {
		t.Fatal("inventory above capacity accepted")
	}
}
