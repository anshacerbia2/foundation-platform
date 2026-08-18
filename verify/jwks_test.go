package verify_test

// The key source is where a hostile or misconfigured issuer response is absorbed. These tests
// are mostly about what it declines to load: a key too short for the profile, a key for another
// algorithm, a key for encryption rather than signing, and a response with nothing usable in it.

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/verify"
)

// jwksServer serves a scripted key set and counts fetches.
type jwksServer struct {
	body    string
	status  int
	fetches atomic.Int32
}

func (s *jwksServer) start(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.fetches.Add(1)
		status := s.status
		if status == 0 {
			status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(s.body))
	}))
	t.Cleanup(server.Close)
	return server
}

func keyEntry(t *testing.T, kid string, key *rsa.PublicKey, extra string) string {
	t.Helper()
	return fmt.Sprintf(`{"kty":"RSA","kid":%q,"n":%q,"e":%q%s}`,
		kid,
		base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1}),
		extra)
}

func newJWKS(t *testing.T, server *httptest.Server, now func() time.Time) *verify.JWKS {
	t.Helper()
	source, err := verify.NewJWKS(verify.JWKSConfig{
		URL:                server.URL,
		HTTPClient:         server.Client(),
		Timeout:            2 * time.Second,
		MinRefetchInterval: time.Minute,
		Now:                now,
	})
	if err != nil {
		t.Fatalf("NewJWKS: %v", err)
	}
	return source
}

// TestConstructionPerformsNoNetworkAccess is STD-GLB-BE-001 rule 11 asserted directly. A key
// source that fetched on construction would make linking it a network call, and the composition
// root could not choose when that happens.
func TestConstructionPerformsNoNetworkAccess(t *testing.T) {
	server := &jwksServer{body: `{"keys":[]}`}
	instance := server.start(t)

	source := newJWKS(t, instance, nil)
	if server.fetches.Load() != 0 {
		t.Errorf("construction performed %d fetches, want 0", server.fetches.Load())
	}
	if source.Len() != 0 {
		t.Errorf("a freshly constructed source holds %d keys, want 0", source.Len())
	}
}

func TestRefetchLoadsASigningKey(t *testing.T) {
	server := &jwksServer{body: `{"keys":[` + keyEntry(t, testKeyID, &signingKey.PublicKey, `,"use":"sig","alg":"PS256"`) + `]}`}
	source := newJWKS(t, server.start(t), nil)

	if err := source.Refetch(); err != nil {
		t.Fatalf("Refetch: %v", err)
	}
	if _, ok := source.Key(testKeyID); !ok {
		t.Fatal("the published key was not loaded")
	}
	if source.Len() != 1 {
		t.Errorf("loaded %d keys, want 1", source.Len())
	}
}

// TestKeysBelowTheProfileMinimumAreDeclined is the assertion that keeps a weak key out of the
// cache entirely. Rejecting it at verification instead would mean the process had already been
// willing to trust it.
func TestKeysBelowTheProfileMinimumAreDeclined(t *testing.T) {
	short, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	server := &jwksServer{body: `{"keys":[` + keyEntry(t, "short-key", &short.PublicKey, "") + `]}`}
	source := newJWKS(t, server.start(t), nil)

	err = source.Refetch()
	if !errors.Is(err, verify.ErrKeysUnavailable) {
		t.Fatalf("Refetch error = %v, want ErrKeysUnavailable for a set with nothing usable", err)
	}
	if _, ok := source.Key("short-key"); ok {
		t.Error("a 2048-bit key entered the cache; the profile minimum is 3072")
	}
}

// TestUnusableEntriesAreSkippedWithoutFailingTheSet covers the mixed response a real issuer
// sends. An issuer legitimately publishes keys for algorithms and purposes this resource does not
// use, and failing the fetch because one entry is unusable would take out verification for the
// keys that are.
func TestUnusableEntriesAreSkippedWithoutFailingTheSet(t *testing.T) {
	short, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	entries := []string{
		keyEntry(t, "encryption-key", &signingKey.PublicKey, `,"use":"enc"`),
		keyEntry(t, "other-algorithm", &signingKey.PublicKey, `,"alg":"RS256"`),
		keyEntry(t, "too-short", &short.PublicKey, `,"use":"sig"`),
		`{"kty":"EC","kid":"elliptic","crv":"P-256","x":"aa","y":"bb"}`,
		`{"kty":"RSA","kid":"","n":"aa","e":"AQAB"}`,
		keyEntry(t, testKeyID, &signingKey.PublicKey, `,"use":"sig","alg":"PS256"`),
	}

	server := &jwksServer{body: `{"keys":[` + join(entries) + `]}`}
	source := newJWKS(t, server.start(t), nil)

	if err := source.Refetch(); err != nil {
		t.Fatalf("Refetch: %v", err)
	}
	if source.Len() != 1 {
		t.Errorf("loaded %d keys, want only the usable one", source.Len())
	}
	for _, declined := range []string{"encryption-key", "other-algorithm", "too-short", "elliptic", ""} {
		if _, ok := source.Key(declined); ok {
			t.Errorf("key %q was loaded and should not have been", declined)
		}
	}
	if _, ok := source.Key(testKeyID); !ok {
		t.Error("the usable signing key was not loaded")
	}
}

// TestRefetchIsRateLimited is the control that stops an unknown key identifier — the one input an
// attacker chooses — from becoming a denial of service against a Tier-0 dependency.
func TestRefetchIsRateLimited(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	server := &jwksServer{body: `{"keys":[` + keyEntry(t, testKeyID, &signingKey.PublicKey, `,"use":"sig"`) + `]}`}
	source := newJWKS(t, server.start(t), clock)

	for range 5 {
		if err := source.Refetch(); err != nil {
			t.Fatalf("Refetch: %v", err)
		}
	}
	if got := server.fetches.Load(); got != 1 {
		t.Errorf("fetched %d times inside the interval, want 1", got)
	}

	now = now.Add(2 * time.Minute)
	if err := source.Refetch(); err != nil {
		t.Fatalf("Refetch after the interval: %v", err)
	}
	if got := server.fetches.Load(); got != 2 {
		t.Errorf("fetched %d times after the interval elapsed, want 2", got)
	}
}

// TestAFailedRefetchDoesNotDiscardAWorkingKeySet is why the cache is replaced only on success.
// Caching an empty result would reject every token until the next permitted refetch, turning a
// transient issuer fault into an outage on this side.
func TestAFailedRefetchDoesNotDiscardAWorkingKeySet(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	server := &jwksServer{body: `{"keys":[` + keyEntry(t, testKeyID, &signingKey.PublicKey, `,"use":"sig"`) + `]}`}
	source := newJWKS(t, server.start(t), clock)

	if err := source.Refetch(); err != nil {
		t.Fatalf("Refetch: %v", err)
	}

	server.status = http.StatusInternalServerError
	now = now.Add(2 * time.Minute)
	if err := source.Refetch(); !errors.Is(err, verify.ErrKeysUnavailable) {
		t.Fatalf("Refetch error = %v, want ErrKeysUnavailable", err)
	}

	if _, ok := source.Key(testKeyID); !ok {
		t.Error("a failed refetch discarded the working key set")
	}
}

// TestRetiredKeysAreRemovedOnASuccessfulRefetch is the other side of the same choice. A merge
// would keep a key the issuer has retired, and a retired key is retired because it should no
// longer verify anything.
func TestRetiredKeysAreRemovedOnASuccessfulRefetch(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	rotated, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	server := &jwksServer{body: `{"keys":[` + keyEntry(t, "old-key", &signingKey.PublicKey, `,"use":"sig"`) + `]}`}
	source := newJWKS(t, server.start(t), clock)

	if err := source.Refetch(); err != nil {
		t.Fatalf("Refetch: %v", err)
	}
	if _, ok := source.Key("old-key"); !ok {
		t.Fatal("the first key was not loaded")
	}

	server.body = `{"keys":[` + keyEntry(t, "new-key", &rotated.PublicKey, `,"use":"sig"`) + `]}`
	now = now.Add(2 * time.Minute)
	if err := source.Refetch(); err != nil {
		t.Fatalf("Refetch after rotation: %v", err)
	}

	if _, ok := source.Key("old-key"); ok {
		t.Error("a retired key survived a successful refetch")
	}
	if _, ok := source.Key("new-key"); !ok {
		t.Error("the rotated key was not loaded")
	}
}

func TestUnreachableAndMalformedResponsesReportUnavailable(t *testing.T) {
	cases := map[string]*jwksServer{
		"server error":  {status: http.StatusInternalServerError, body: `{}`},
		"not found":     {status: http.StatusNotFound, body: `{}`},
		"not json":      {body: `this is not a key set`},
		"empty set":     {body: `{"keys":[]}`},
		"no keys field": {body: `{}`},
	}

	for name, server := range cases {
		t.Run(name, func(t *testing.T) {
			source := newJWKS(t, server.start(t), nil)
			if err := source.Refetch(); !errors.Is(err, verify.ErrKeysUnavailable) {
				t.Fatalf("Refetch error = %v, want ErrKeysUnavailable", err)
			}
		})
	}
}

func TestNewJWKSRequiresAURL(t *testing.T) {
	if _, err := verify.NewJWKS(verify.JWKSConfig{}); err == nil {
		t.Fatal("NewJWKS accepted an empty URL")
	}
}

// TestEndToEndAgainstAPublishedKeySet joins the two halves: a token signed by the issuer's key
// verifies once the source has loaded it from the endpoint.
func TestEndToEndAgainstAPublishedKeySet(t *testing.T) {
	server := &jwksServer{body: `{"keys":[` + keyEntry(t, testKeyID, &signingKey.PublicKey, `,"use":"sig","alg":"PS256"`) + `]}`}
	source := newJWKS(t, server.start(t), nil)

	v, err := verify.New(verify.Config{
		Issuer: testIssuer, Audience: testAudience, Keys: source,
		Requirement: requirement, Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// No explicit Refetch. The first verification finds the identifier unknown and loads the
	// set, which is how a cold replica serves its first request.
	if _, err := v.Verify(sign(t, signingKey, validHeader(), validPayload())); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if server.fetches.Load() != 1 {
		t.Errorf("fetched %d times, want 1", server.fetches.Load())
	}
}

func join(entries []string) string {
	out := ""
	for i, entry := range entries {
		if i > 0 {
			out += ","
		}
		out += entry
	}
	return out
}
