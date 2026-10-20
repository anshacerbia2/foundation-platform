package idempotency

// The stored response, beside the live one.
//
// A command whose answer carries a secret, such as an invitation token or a first credential,
// answers the secret live and never records it. STD-GLB-001 1.7.0: "A secret the command answers
// once is never in the idempotency record." OWASP puts the reason in one line: "The best way to
// protect sensitive information is to not store it in the first place" (Cryptographic Storage
// Cheat Sheet). A retry with the same key then answers the same result without the secret, and a
// lost secret is revoked and issued again.
//
// Withhold derives the stored body from the live one, so the two cannot drift apart except by the
// members withheld. A consumer that rendered the stored body separately would have two renderers,
// and the day a field was added to one and not the other, a replay would answer something the
// first response did not.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/anshacerbia2/foundation-platform/db"
)

var (
	// ErrNotWithheld is a pointer that names no member of the body. It fails rather than records
	// the body unchanged: a secret's member renamed in the response, and not here, would otherwise
	// be recorded under its new name while every call still succeeded.
	ErrNotWithheld = errors.New("idempotency: a withheld member is not in the response")

	// ErrNothingWithheld is CompleteWithheld called without a pointer. Complete records a body
	// unchanged; a caller that means that calls it.
	ErrNothingWithheld = errors.New("idempotency: no member to withhold was named")

	// ErrInvalidPointer is a pointer that is not an RFC 6901 JSON Pointer to a member.
	ErrInvalidPointer = errors.New("idempotency: invalid JSON Pointer")
)

// Withhold returns body without the members the pointers name: the response a replay may answer.
//
// Each pointer is an RFC 6901 JSON Pointer, such as "/token" or "/credential/value". It must name
// an object member present in body; a pointer to the whole document, to an array element, or to
// nothing is refused. Numbers keep their exact text. Object members come back in sorted order,
// which is the order PostgreSQL's jsonb returns them in anyway.
func Withhold(body json.RawMessage, pointers ...string) (json.RawMessage, error) {
	document, err := decodeDocument(body)
	if err != nil {
		return nil, err
	}
	for _, pointer := range pointers {
		if err := removeMember(document, pointer); err != nil {
			return nil, err
		}
	}
	stored, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("idempotency: encoding the stored response: %w", err)
	}
	return stored, nil
}

// CompleteWithheld is Complete for a response that carries a secret answered once. It records body
// without the members withheld names; the caller sends body itself, live, and only once.
//
// A replay of the key then answers the recorded body, without the secret, under the status the
// first response had.
func CompleteWithheld(ctx context.Context, tx db.Tx, scope, key, digest string, status int,
	body json.RawMessage, withheld ...string) error {
	if len(withheld) == 0 {
		return ErrNothingWithheld
	}
	if err := validate(tx, scope, key, digest); err != nil {
		return err
	}
	stored, err := Withhold(body, withheld...)
	if err != nil {
		return err
	}
	return Complete(ctx, tx, scope, key, digest, status, stored)
}

func decodeDocument(body json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, errors.New("idempotency: response body must be valid JSON")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("idempotency: response body must be one JSON value")
	}
	return document, nil
}

// removeMember deletes the object member pointer names.
//
// RFC 6901 §4: a reference token is decoded by "first transforming any occurrence of the sequence
// '~1' to '/', and then transforming any occurrence of the sequence '~0' to '~'", and evaluation
// "will raise an error condition if it fails to resolve a concrete value for any of the JSON
// pointer's reference tokens".
func removeMember(document any, pointer string) error {
	if pointer == "" || !strings.HasPrefix(pointer, "/") {
		return fmt.Errorf("%w: %q must name a member, starting with /", ErrInvalidPointer, pointer)
	}
	tokens := strings.Split(pointer[1:], "/")
	for i, token := range tokens {
		if strings.Contains(strings.NewReplacer("~0", "", "~1", "").Replace(token), "~") {
			return fmt.Errorf("%w: %q has a ~ that is not ~0 or ~1", ErrInvalidPointer, pointer)
		}
		tokens[i] = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
	}

	current := document
	for _, token := range tokens[:len(tokens)-1] {
		switch value := current.(type) {
		case map[string]any:
			next, ok := value[token]
			if !ok {
				return fmt.Errorf("%w: %q", ErrNotWithheld, pointer)
			}
			current = next
		case []any:
			index, ok := arrayIndex(token, len(value))
			if !ok {
				return fmt.Errorf("%w: %q", ErrNotWithheld, pointer)
			}
			current = value[index]
		default:
			return fmt.Errorf("%w: %q", ErrNotWithheld, pointer)
		}
	}

	parent, ok := current.(map[string]any)
	if _, inArray := current.([]any); inArray {
		return fmt.Errorf("%w: %q names an array element, not an object member", ErrInvalidPointer, pointer)
	}
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotWithheld, pointer)
	}
	last := tokens[len(tokens)-1]
	if _, ok := parent[last]; !ok {
		return fmt.Errorf("%w: %q", ErrNotWithheld, pointer)
	}
	delete(parent, last)
	return nil
}

// arrayIndex reads an RFC 6901 array index: "0", or digits without a leading zero.
func arrayIndex(token string, length int) (int, bool) {
	if token == "" || (len(token) > 1 && token[0] == '0') {
		return 0, false
	}
	for _, r := range token {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	index, err := strconv.Atoi(token)
	if err != nil || index >= length {
		return 0, false
	}
	return index, true
}
