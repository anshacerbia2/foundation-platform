package outbox

// The dispatcher's database contract, verified before any worker starts.
//
// # Why this exists
//
// The dispatcher's SQL lives here, and the schema it needs is applied by whichever service owns
// the database it drains — a different repository, on a different release cadence. Nothing links
// the two. That gap has already produced one incompatibility: this module gained
// platform.delivery_receipt and began writing to it, while the consuming service was still a
// version behind and its database had no such table.
//
// Without a preflight the symptom is per-event and misleading. The receipt write shares a
// transaction with the row being marked published, so a missing table means no row is ever marked
// published, every event fails, and the log fills with an error about a table nobody deployed.
//
// # Why the contract is declared here rather than by the caller
//
// A prerequisite list maintained by each consumer moves the skew rather than removing it: the next
// dependency this module adds would be missing from every consumer's copy. The list belongs beside
// the statements that create the dependency, so a change here reaches every consumer with the
// version that introduced it.
//
// # Why Run enforces it
//
// An exported check the composition root must remember to call is a check the next composition root
// forgets. Run is the one function nobody can skip, and it already takes a context and returns an
// error. CheckDispatcherPrerequisites stays exported for readiness probes and manual preflight, but
// nothing depends on it being called.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anshacerbia2/foundation-platform/db"
)

// ErrPrerequisite means the database this dispatcher was pointed at does not satisfy the contract
// its statements require. It is not retryable: no amount of waiting adds a missing table or grant.
var ErrPrerequisite = errors.New("outbox: the database does not satisfy the dispatcher's contract")

// requirement is one table and the privileges the dispatcher's statements exercise on it.
//
// Every entry names the statement that needs it, because a requirement whose reason is not written
// down is one someone removes to make a deployment start.
var dispatcherRequirements = []struct {
	table      string
	privileges []string
	because    string
}{
	{
		table:      "platform.outbox",
		privileges: []string{"SELECT", "UPDATE"},
		because:    "claimStatement reads the batch; markPublished and recordFailure write the outcome back",
	},
	{
		table:      "platform.dead_letter",
		privileges: []string{"INSERT", "SELECT"},
		// SELECT is not for reading incidents. A conflict target makes PostgreSQL require SELECT
		// on the table being inserted into, which is measured rather than assumed — see
		// organization-control's dispatch-role capability test.
		because: "deadLetterStatement inserts the incident, and its ON CONFLICT target requires SELECT",
	},
	{
		table:      "platform.delivery_receipt",
		privileges: []string{"INSERT"},
		because:    "recordReceiptStatement records what each delivery established",
	},
}

// CheckDispatcherPrerequisites reports whether the database satisfies the dispatcher's contract.
//
// It answers for the connection it is given, as the role that connection uses: `current_user` is
// the subject of every privilege question here, so a check that passes for the owner says nothing
// about the role the dispatcher will actually run as.
//
// Errors are classified. A database that cannot be reached is not a database missing a migration,
// and reporting the second for the first sends an operator looking for a deployment that never
// went wrong. Connectivity failures are returned as themselves; contract failures wrap
// ErrPrerequisite.
// The parameter is *db.Pool rather than the package's unexported transactor interface: this is an
// exported entry point, and a signature naming a type the caller cannot write is not usable from
// outside. Run passes its own transactor through the shared implementation below.
func CheckDispatcherPrerequisites(ctx context.Context, pool *db.Pool) error {
	if pool == nil {
		return errors.New("outbox: a pool is required")
	}
	return checkPrerequisites(ctx, pool)
}

func checkPrerequisites(ctx context.Context, tx transactor) error {
	if tx == nil {
		return errors.New("outbox: a transactor is required")
	}

	var problems []string
	err := tx.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		// Schema USAGE first. A table privilege inside a schema the role cannot enter is not
		// usable, and the resulting error names the table rather than the schema — which is the
		// wrong object to go looking at.
		var mayEnter bool
		if err := tx.QueryRow(ctx,
			`SELECT has_schema_privilege(current_user, 'platform', 'USAGE')`).Scan(&mayEnter); err != nil {
			return err
		}
		if !mayEnter {
			problems = append(problems,
				"no USAGE on schema platform, so every table privilege below it is unusable")
		}

		for _, required := range dispatcherRequirements {
			var present bool
			if err := tx.QueryRow(ctx,
				`SELECT to_regclass($1) IS NOT NULL`, required.table).Scan(&present); err != nil {
				return err
			}
			if !present {
				problems = append(problems, fmt.Sprintf(
					"%s does not exist; the foundation-platform migrations for this version have not been applied (%s)",
					required.table, required.because))
				continue
			}

			for _, privilege := range required.privileges {
				var permitted bool
				if err := tx.QueryRow(ctx,
					`SELECT has_table_privilege(current_user, $1, $2)`,
					required.table, privilege).Scan(&permitted); err != nil {
					return err
				}
				if !permitted {
					problems = append(problems, fmt.Sprintf(
						"no %s on %s; the grant for this role was not applied (%s)",
						privilege, required.table, required.because))
				}
			}
		}
		return nil
	})
	if err != nil {
		// Reached the database and something else went wrong, or did not reach it at all. Either
		// way it is not a statement about the contract, and it must not be reported as one.
		return fmt.Errorf("outbox: the dispatcher's prerequisites could not be checked: %w", err)
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w:\n  - %s", ErrPrerequisite, strings.Join(problems, "\n  - "))
	}
	return nil
}
