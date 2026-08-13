// Package migrations carries the platform schema and hands it to the systems that own a
// database.
//
// This module owns no database. It is a library with no deployable, and EAD-003 forbids
// cross-domain persistence, so there is nowhere for one to live. The platform schema
// exists once inside each consuming database — identity-control's and
// organization-control's — and those two copies are unrelated and never joined. The
// shared name reflects shared code, not shared storage.
//
// The SQL lives here rather than in each consuming repository because it must move in
// lockstep with the code that queries it. A column added to platform.outbox and a change
// to outbox.Append are one change; separating them across repositories allows a
// deployment where one has shipped and the other has not.
//
// Embedding it means a consumer receives the schema through go.mod, pinned to the same
// version as the dispatcher that reads it. Copying the file would recreate the divergence
// this module exists to prevent.
//
// This package does not apply anything. Applying the schema is DDL, and
// TDD-foundation-platform-001 requires migration to run under a role distinct from the
// runtime role, which holds no DDL privilege. That role belongs to the consuming system's
// migration job, not to a library linked into its application.
package migrations

import (
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed platform/*.sql
var embedded embed.FS

// Platform is the platform schema as a filesystem, rooted at the directory holding the
// migration files.
//
// It is exposed as fs.FS so a consumer can hand it to whichever runner it already uses —
// Atlas, a versioned migration tool, or its own — without this package taking a position
// on which.
var Platform = platformRoot()

// Migration is one migration file.
type Migration struct {
	// Name is the file name, whose numeric prefix determines application order.
	Name string

	// SQL is the file's contents. It may contain several statements.
	SQL string
}

// PlatformMigrations returns the platform schema migrations in application order.
//
// Order comes from the file name rather than from a manifest, so a file that is added
// without being registered cannot be silently skipped. There is no second list to forget
// to update.
func PlatformMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(Platform, ".")
	if err != nil {
		return nil, fmt.Errorf("migrations: reading platform schema: %w", err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	migrations := make([]Migration, 0, len(names))
	for _, name := range names {
		b, err := fs.ReadFile(Platform, name)
		if err != nil {
			return nil, fmt.Errorf("migrations: reading %s: %w", name, err)
		}
		migrations = append(migrations, Migration{Name: name, SQL: string(b)})
	}

	if len(migrations) == 0 {
		return nil, fmt.Errorf("migrations: the platform schema is empty")
	}
	return migrations, nil
}

// platformRoot re-roots the embedded filesystem at the platform directory.
//
// fs.Sub fails only on a malformed path, and this one is a compile-time constant that
// go:embed has already resolved, so the error is unreachable. It panics rather than
// returning an error because a package whose embedded content is missing cannot do
// anything useful, and discovering that at import is better than at first use.
func platformRoot() fs.FS {
	sub, err := fs.Sub(embedded, "platform")
	if err != nil {
		panic(fmt.Sprintf("migrations: embedded platform schema is unreadable: %v", err))
	}
	return sub
}
