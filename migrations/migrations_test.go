package migrations

import (
	"io/fs"
	"sort"
	"strings"
	"testing"
)

func TestPlatformMigrationsAreNotEmpty(t *testing.T) {
	got, err := PlatformMigrations()
	if err != nil {
		t.Fatalf("PlatformMigrations: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("no migrations were embedded")
	}

	for _, m := range got {
		if strings.TrimSpace(m.SQL) == "" {
			t.Errorf("%s is empty", m.Name)
		}
		if !strings.HasSuffix(m.Name, ".sql") {
			t.Errorf("%s is not a .sql file", m.Name)
		}
	}
}

// Order is the whole contract. A migration set applied out of order fails at best and
// produces a schema that differs from the one the tests ran against at worst.
func TestPlatformMigrationsAreOrderedByName(t *testing.T) {
	got, err := PlatformMigrations()
	if err != nil {
		t.Fatalf("PlatformMigrations: %v", err)
	}

	names := make([]string, len(got))
	for i, m := range got {
		names[i] = m.Name
	}

	sorted := append([]string(nil), names...)
	sort.Strings(sorted)

	for i := range names {
		if names[i] != sorted[i] {
			t.Fatalf("migrations are not in name order: %v", names)
		}
	}
}

// A numeric prefix is what makes name order the same as application order. Without it,
// "add_index.sql" would apply before "create_table.sql".
func TestEveryMigrationCarriesANumericPrefix(t *testing.T) {
	got, err := PlatformMigrations()
	if err != nil {
		t.Fatalf("PlatformMigrations: %v", err)
	}

	for _, m := range got {
		if len(m.Name) < 4 {
			t.Errorf("%s is too short to carry a version prefix", m.Name)
			continue
		}
		for _, c := range m.Name[:4] {
			if c < '0' || c > '9' {
				t.Errorf("%s does not start with a four-digit version prefix", m.Name)
				break
			}
		}
	}
}

// The tables outbox, inbox, and idempotency query must exist in the schema this package
// ships, or those packages compile against a database that cannot hold them.
func TestPlatformSchemaDeclaresTheTablesTheLibraryQueries(t *testing.T) {
	got, err := PlatformMigrations()
	if err != nil {
		t.Fatalf("PlatformMigrations: %v", err)
	}

	var all string
	for _, m := range got {
		all += m.SQL
	}

	for _, table := range []string{
		"platform.outbox",
		"platform.processed_event",
		"platform.dead_letter",
		"platform.idempotency_key",
		"platform.outbox_sequence",
	} {
		if !strings.Contains(all, table) {
			t.Errorf("the platform schema never mentions %s", table)
		}
	}
}

// Platform is exported for runners that take a filesystem rather than a slice, so it has
// to be rooted at the files themselves rather than at the directory containing them.
func TestPlatformFilesystemIsRootedAtTheMigrations(t *testing.T) {
	entries, err := fs.ReadDir(Platform, ".")
	if err != nil {
		t.Fatalf("reading Platform: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("Platform is rooted above the migration files")
	}

	for _, e := range entries {
		if e.IsDir() {
			t.Errorf("Platform holds a directory %q; it should hold files only", e.Name())
		}
	}
}
