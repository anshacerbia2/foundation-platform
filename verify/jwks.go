package verify

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// maxJWKSBytes bounds a key set response.
//
// A realm publishes a handful of keys, so this is generous by orders of magnitude. It exists
// because the issuer is a dependency whose response size this process does not control.
const maxJWKSBytes = 1 << 20

// minRSABits is the smallest modulus this verifier will use.
//
// STD-IAM-002 §3.2.2 requires at least 3072 bits. A shorter key is refused at load rather than
// at verification, so a weak key never enters the cache: rejecting it later would mean the
// process had already been willing to trust it.
const minRSABits = 3072

// JWKSConfig configures the key source.
type JWKSConfig struct {
	// URL is the issuer's published key set. It is configuration and is never taken from a
	// token: a token that named its own key source would choose the key that validates it.
	URL string

	// HTTPClient performs the fetch. A nil client gets one with Timeout.
	HTTPClient *http.Client

	// Timeout bounds one fetch.
	Timeout time.Duration

	// MinRefetchInterval rate limits reloading after an unknown key identifier.
	//
	// This is the control that stops a stream of random identifiers becoming a denial of
	// service against the issuer, which matters more here than anywhere else: the issuer is
	// Tier-0 and every domain depends on it.
	MinRefetchInterval time.Duration

	// Now is the clock. A test supplies its own.
	Now func() time.Time
}

func (c *JWKSConfig) applyDefaults() {
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MinRefetchInterval <= 0 {
		c.MinRefetchInterval = 60 * time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: c.Timeout}
	}
}

// JWKS is a caching key source backed by an issuer's published key set.
type JWKS struct {
	cfg JWKSConfig

	mu          sync.RWMutex
	keys        map[string]*rsa.PublicKey
	lastFetched time.Time
}

// NewJWKS constructs the key source. It performs no network access: nothing starts by being
// linked or constructed, per STD-GLB-BE-001 rule 10. Call Refetch, or let the first unknown key
// identifier trigger it.
func NewJWKS(cfg JWKSConfig) (*JWKS, error) {
	cfg.applyDefaults()
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("verify: a JWKS URL is required")
	}
	return &JWKS{cfg: cfg, keys: map[string]*rsa.PublicKey{}}, nil
}

var _ KeySource = (*JWKS)(nil)

// Key returns a cached key.
func (j *JWKS) Key(kid string) (*rsa.PublicKey, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	key, ok := j.keys[kid]
	return key, ok
}

// Refetch reloads the key set, subject to the rate limit.
//
// A call inside the interval returns nil without fetching. That is not a silent failure: the
// caller's next Key lookup reports the identifier still unknown, and the verifier then rejects
// the token. Refusing is correct — the alternative is letting one attacker-chosen identifier
// drive a fetch per request against a Tier-0 dependency.
func (j *JWKS) Refetch() error {
	j.mu.Lock()
	if !j.lastFetched.IsZero() && j.cfg.Now().Sub(j.lastFetched) < j.cfg.MinRefetchInterval {
		j.mu.Unlock()
		return nil
	}
	// The timestamp is recorded before the fetch, so a slow or failing issuer cannot be
	// hammered by concurrent verifications each waiting to be the one that retries.
	j.lastFetched = j.cfg.Now()
	j.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), j.cfg.Timeout)
	defer cancel()

	keys, err := j.fetch(ctx)
	if err != nil {
		return err
	}

	j.mu.Lock()
	// Replaced wholesale rather than merged. A merge would keep a key the issuer has retired,
	// and a retired key is retired because it should no longer verify anything.
	j.keys = keys
	j.mu.Unlock()
	return nil
}

// Len reports how many keys are cached, for a readiness check or a metric.
func (j *JWKS) Len() int {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return len(j.keys)
}

func (j *JWKS) fetch(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, j.cfg.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("verify: build JWKS request: %w", ErrKeysUnavailable)
	}
	request.Header.Set("Accept", "application/json")

	response, err := j.cfg.HTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("verify: JWKS endpoint unreachable: %w", ErrKeysUnavailable)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("verify: JWKS endpoint returned status %d: %w",
			response.StatusCode, ErrKeysUnavailable)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxJWKSBytes))
	if err != nil {
		return nil, fmt.Errorf("verify: JWKS response was truncated: %w", ErrKeysUnavailable)
	}

	var document struct {
		Keys []struct {
			KeyType   string `json:"kty"`
			KeyID     string `json:"kid"`
			Use       string `json:"use"`
			Algorithm string `json:"alg"`
			Modulus   string `json:"n"`
			Exponent  string `json:"e"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, fmt.Errorf("verify: JWKS is not a key set: %w", ErrKeysUnavailable)
	}

	keys := map[string]*rsa.PublicKey{}
	for _, entry := range document.Keys {
		// Every skip below is a key this verifier declines to trust rather than an error that
		// discards the whole set. An issuer legitimately publishes keys for algorithms and
		// purposes this resource does not use, and failing the fetch because one entry is
		// unusable would take out verification for the keys that are.
		switch {
		case entry.KeyType != "RSA":
			continue
		case entry.KeyID == "":
			continue
		case entry.Use != "" && entry.Use != "sig":
			continue
		case entry.Algorithm != "" && entry.Algorithm != permittedAlgorithm:
			continue
		}

		key, err := parseRSAKey(entry.Modulus, entry.Exponent)
		if err != nil {
			continue
		}
		keys[entry.KeyID] = key
	}

	if len(keys) == 0 {
		// An empty result is reported rather than cached. Caching it would replace a working
		// key set with nothing and reject every token until the next permitted refetch.
		return nil, fmt.Errorf("verify: JWKS carried no usable %s signing key: %w",
			permittedAlgorithm, ErrKeysUnavailable)
	}
	return keys, nil
}

// parseRSAKey builds a public key from the base64url modulus and exponent.
func parseRSAKey(modulus, exponent string) (*rsa.PublicKey, error) {
	if modulus == "" || exponent == "" {
		return nil, errors.New("verify: key is missing n or e")
	}

	rawModulus, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(modulus, "="))
	if err != nil {
		return nil, errors.New("verify: modulus is not base64url")
	}
	rawExponent, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(exponent, "="))
	if err != nil {
		return nil, errors.New("verify: exponent is not base64url")
	}
	if len(rawExponent) == 0 || len(rawExponent) > 8 {
		return nil, errors.New("verify: exponent is out of range")
	}

	key := &rsa.PublicKey{
		N: new(big.Int).SetBytes(rawModulus),
		E: int(new(big.Int).SetBytes(rawExponent).Int64()),
	}

	if key.N.Sign() <= 0 {
		return nil, errors.New("verify: modulus is not positive")
	}
	if key.N.BitLen() < minRSABits {
		return nil, fmt.Errorf("verify: modulus is %d bits, below the %d-bit minimum",
			key.N.BitLen(), minRSABits)
	}
	// A public exponent below 3, or an even one, is not a usable RSA exponent. Rejecting it
	// here keeps a malformed or hostile key set from reaching the verification path.
	if key.E < 3 || key.E%2 == 0 {
		return nil, fmt.Errorf("verify: public exponent %d is not usable", key.E)
	}
	return key, nil
}

// StaticKeys is a KeySource holding a fixed set, for a test or a deployment that distributes
// verification material by configuration rather than by endpoint.
type StaticKeys map[string]*rsa.PublicKey

// Key satisfies KeySource.
func (s StaticKeys) Key(kid string) (*rsa.PublicKey, bool) {
	key, ok := s[kid]
	return key, ok
}

// Refetch satisfies KeySource and does nothing: a static set has nowhere to reload from.
func (s StaticKeys) Refetch() error { return nil }

var _ KeySource = StaticKeys(nil)
