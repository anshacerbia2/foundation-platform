// Package idempotency claims mutation keys and replays completed responses without
// re-executing their operations.
package idempotency

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/anshacerbia2/foundation-platform/db"
)

var (
	ErrNoTransaction = errors.New("idempotency: a transaction handle is required")
	ErrNoScope       = errors.New("idempotency: caller scope is required")
	ErrNoKey         = errors.New("idempotency: key is required")
	ErrNoDigest      = errors.New("idempotency: request digest is required")
	ErrConflict      = errors.New("idempotency: key was used for a different request")
	ErrInProgress    = errors.New("idempotency: request is already in progress")
	ErrNotClaimed    = errors.New("idempotency: key has not been claimed with this digest")
	ErrScopeTooLong  = errors.New("idempotency: caller scope exceeds 512 bytes")
	ErrKeyTooLong    = errors.New("idempotency: key exceeds 255 bytes")
	ErrDigestTooLong = errors.New("idempotency: request digest exceeds 256 bytes")
)

type State uint8

const (
	StateClaimed State = iota + 1
	StateReplay
	StateInProgress
)

// Result is the decision produced by Claim.
type Result struct {
	State  State
	Status int
	Body   json.RawMessage
}

const claimStatement = `INSERT INTO platform.idempotency_key (scope, key, request_digest)
VALUES ($1, $2, $3)
ON CONFLICT (scope, key) DO NOTHING`

const readStatement = `SELECT request_digest, response_status, response_body, completed_at IS NOT NULL
FROM platform.idempotency_key
WHERE scope = $1 AND key = $2`

// Digest returns a stable SHA-256 digest for the exact request bytes supplied by a
// caller. The caller decides which method, target, and body bytes belong in the input.
func Digest(parts ...[]byte) string {
	h := sha256.New()
	for _, part := range parts {
		var size [8]byte
		for i := range size {
			size[7-i] = byte(uint64(len(part)) >> (i * 8))
		}
		h.Write(size[:])
		h.Write(part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Claim reserves a key per authenticated caller scope inside the caller's transaction.
func Claim(ctx context.Context, tx db.Tx, scope, key, digest string) (Result, error) {
	if err := validate(tx, scope, key, digest); err != nil {
		return Result{}, err
	}
	scope, key, digest = strings.TrimSpace(scope), strings.TrimSpace(key), strings.TrimSpace(digest)

	tag, err := tx.Exec(ctx, claimStatement, scope, key, digest)
	if err != nil {
		return Result{}, fmt.Errorf("idempotency: claiming key %q: %w", key, err)
	}
	if tag.RowsAffected() == 1 {
		return Result{State: StateClaimed}, nil
	}

	var (
		storedDigest string
		status       sql.NullInt64
		body         []byte
		completed    bool
	)
	if err := tx.QueryRow(ctx, readStatement, scope, key).Scan(&storedDigest, &status, &body, &completed); err != nil {
		return Result{}, fmt.Errorf("idempotency: reading existing key %q: %w", key, err)
	}
	if storedDigest != digest {
		return Result{}, ErrConflict
	}
	if !completed || !status.Valid {
		return Result{State: StateInProgress}, ErrInProgress
	}
	return Result{State: StateReplay, Status: int(status.Int64), Body: append(json.RawMessage(nil), body...)}, nil
}

const completeStatement = `UPDATE platform.idempotency_key
SET response_status = $4, response_body = $5, completed_at = now()
WHERE scope = $1 AND key = $2 AND request_digest = $3 AND completed_at IS NULL`

// Complete stores a response for future replay in the transaction that completed the
// mutation.
func Complete(ctx context.Context, tx db.Tx, scope, key, digest string, status int, body json.RawMessage) error {
	if err := validate(tx, scope, key, digest); err != nil {
		return err
	}
	if status < 100 || status > 599 {
		return fmt.Errorf("idempotency: invalid HTTP status %d", status)
	}
	if len(body) == 0 || !json.Valid(body) {
		return errors.New("idempotency: response body must be valid JSON")
	}

	tag, err := tx.Exec(ctx, completeStatement,
		strings.TrimSpace(scope), strings.TrimSpace(key), strings.TrimSpace(digest), status, []byte(body))
	if err != nil {
		return fmt.Errorf("idempotency: completing key %q: %w", key, err)
	}
	if tag.RowsAffected() != 1 {
		return ErrNotClaimed
	}
	return nil
}

func validate(tx db.Tx, scope, key, digest string) error {
	switch {
	case db.IsNilTx(tx):
		return ErrNoTransaction
	case strings.TrimSpace(scope) == "":
		return ErrNoScope
	case strings.TrimSpace(key) == "":
		return ErrNoKey
	case strings.TrimSpace(digest) == "":
		return ErrNoDigest
	case len(strings.TrimSpace(scope)) > 512:
		return ErrScopeTooLong
	case len(strings.TrimSpace(key)) > 255:
		return ErrKeyTooLong
	case len(strings.TrimSpace(digest)) > 256:
		return ErrDigestTooLong
	default:
		return nil
	}
}
