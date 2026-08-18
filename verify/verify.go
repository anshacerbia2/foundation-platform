// Package verify validates a bearer token locally, without a call to the issuer.
//
// Local verification is the property EAD-006 §8 depends on: an identity outage degrades the
// estate rather than stopping it, because every protected resource decides from cached signing
// material and the claims in front of it. A verifier that called the issuer per request would
// make every domain a synchronous dependent of Identity.
//
// # Why this lives in the shared module
//
// Every consuming system verifies tokens. A verifier implemented twice produces two
// verification behaviours while both report compliance, which is the failure this module exists
// to prevent. The propagation budget has the same character and is here for the same reason.
//
// # What this package deliberately does not know
//
// It names no domain concept. STD-IAM-002 §3.5 requires an internal-audience token to carry the
// enterprise subject identifier, and that rule cannot be expressed here without naming a claim
// this module is forbidden from knowing. So the mechanism lives here and the requirement is
// supplied by the consumer through ClaimRequirement, exactly as db.SessionBinder supplies the
// tenant predicate that db is forbidden from writing.
//
// The consequence is worth stating plainly: this package alone does not satisfy STD-IAM-002
// §3.5. A consumer that supplies no requirement gets signature, issuer, audience, and expiry
// checking and nothing else, so Config refuses to build without one.
package verify

import (
	"crypto"
	"crypto/rsa"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Errors a caller distinguishes. They are separate because the responses differ: a malformed
// token is a client defect, an unknown key may be a rotation in progress, and an unavailable
// key source is an outage.
var (
	ErrMalformed        = errors.New("verify: token is malformed")
	ErrAlgorithm        = errors.New("verify: token algorithm is not permitted")
	ErrSignature        = errors.New("verify: signature is invalid")
	ErrIssuer           = errors.New("verify: issuer does not match")
	ErrAudience         = errors.New("verify: audience does not include this resource")
	ErrExpired          = errors.New("verify: token has expired")
	ErrNotYetValid      = errors.New("verify: token is not yet valid")
	ErrUnknownKey       = errors.New("verify: signing key is unknown")
	ErrClaimRequirement = errors.New("verify: token does not satisfy the required claims")
	ErrKeysUnavailable  = errors.New("verify: signing material is unavailable")
)

// permittedAlgorithm is the only algorithm this verifier accepts.
//
// One algorithm, per STD-IAM-002 §3.2.2. A second accepted algorithm is a branch in this
// function, and algorithm confusion is a defect class that lives in exactly that branch.
const permittedAlgorithm = "PS256"

// maxSkewCeiling bounds what a caller may configure.
//
// STD-IAM-002 §3.5 caps clock skew at 60 seconds. A deployment that wanted more would be
// widening the window in which an expired token is still accepted, so the ceiling is enforced
// here rather than trusted to configuration review.
const maxSkewCeiling = 60 * time.Second

// Claims is the verified claim set.
//
// The registered claims are typed. Everything else stays raw and is read by name, which is how
// a consumer reaches its own enterprise claims without this package declaring them.
type Claims struct {
	Issuer    string
	Subject   string
	Audience  []string
	ExpiresAt time.Time
	IssuedAt  time.Time
	NotBefore time.Time

	raw map[string]json.RawMessage
}

// String returns a string claim by name.
func (c Claims) String(name string) (string, bool) {
	encoded, ok := c.raw[name]
	if !ok {
		return "", false
	}
	var value string
	if err := json.Unmarshal(encoded, &value); err != nil {
		return "", false
	}
	return value, value != ""
}

// Int64 returns an integer claim by name.
func (c Claims) Int64(name string) (int64, bool) {
	encoded, ok := c.raw[name]
	if !ok {
		return 0, false
	}
	var value int64
	if err := json.Unmarshal(encoded, &value); err != nil {
		return 0, false
	}
	return value, true
}

// Raw returns an unparsed claim by name, for a shape the helpers above do not cover.
func (c Claims) Raw(name string) (json.RawMessage, bool) {
	encoded, ok := c.raw[name]
	return encoded, ok
}

// Has reports whether a claim is present.
func (c Claims) Has(name string) bool {
	_, ok := c.raw[name]
	return ok
}

// ClaimRequirement is the consumer's own rule, applied after every mechanical check passes.
//
// It runs last on purpose. A requirement evaluated against an unverified token would be reading
// attacker-supplied input, so by the time it runs the signature, issuer, audience, and expiry
// are already established.
type ClaimRequirement interface {
	Require(claims Claims) error
}

// RequirementFunc adapts a function to ClaimRequirement.
type RequirementFunc func(Claims) error

// Require satisfies ClaimRequirement.
func (f RequirementFunc) Require(claims Claims) error { return f(claims) }

// KeySource supplies verification material by key identifier.
//
// An interface so the JWKS client can be replaced in a test by a source holding one key, and so
// a deployment could supply material from somewhere other than an HTTP endpoint. Refetch is
// called at most once per verification, when Key reports the identifier unknown.
type KeySource interface {
	// Key returns the public key for a key identifier.
	Key(kid string) (*rsa.PublicKey, bool)

	// Refetch reloads the key set. It is rate limited by the implementation: an unknown
	// identifier is the one input an attacker controls, so an unbounded refetch would turn a
	// stream of random identifiers into a denial of service against the issuer.
	Refetch() error
}

// Config configures a Verifier.
type Config struct {
	// Issuer is compared for exact equality. A prefix or suffix match would accept a token
	// from `https://issuer.example.com.attacker.test`.
	Issuer string

	// Audience is this resource's registered identifier, which the token's audience must
	// contain.
	Audience string

	// Keys supplies verification material.
	Keys KeySource

	// Requirement is the consumer's claim rule. It is mandatory: without it this verifier
	// checks the mechanics and nothing about what the claims mean, and STD-IAM-002 §3.5 is
	// not satisfied by the mechanics alone.
	Requirement ClaimRequirement

	// MaxSkew tolerates clock drift, capped at 60 seconds.
	MaxSkew time.Duration

	// Now is the clock. A test supplies its own; production leaves it nil.
	Now func() time.Time
}

func (c *Config) applyDefaults() {
	if c.MaxSkew <= 0 {
		c.MaxSkew = 30 * time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

func (c Config) validate() error {
	switch {
	case strings.TrimSpace(c.Issuer) == "":
		return errors.New("verify: an issuer is required")
	case strings.TrimSpace(c.Audience) == "":
		return errors.New("verify: an audience is required")
	case c.Keys == nil:
		return errors.New("verify: a key source is required")
	case c.Requirement == nil:
		return errors.New(
			"verify: a ClaimRequirement is required; without one this verifier checks the " +
				"mechanics only and does not satisfy STD-IAM-002 §3.5")
	case c.MaxSkew > maxSkewCeiling:
		return fmt.Errorf("verify: MaxSkew %s exceeds the 60s ceiling", c.MaxSkew)
	}
	return nil
}

// Verifier validates tokens against one issuer and one audience.
type Verifier struct {
	cfg Config
}

// New constructs a Verifier.
func New(cfg Config) (*Verifier, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Verifier{cfg: cfg}, nil
}

// header is the subset of the JOSE header this verifier reads.
//
// The fields that let a token nominate its own verification material are absent from this
// struct rather than parsed and ignored. Honouring `jku`, `x5u`, `jwk`, or `x5c` lets the
// presenter of a token choose the key that validates it, which removes the signature as a
// control entirely — so they are unreachable from here by construction.
type header struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
	Type      string `json:"typ"`
}

// registeredClaims is the typed subset. Audience is decoded separately because RFC 7519 permits
// either a string or an array.
type registeredClaims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	ExpiresAt *int64 `json:"exp"`
	IssuedAt  *int64 `json:"iat"`
	NotBefore *int64 `json:"nbf"`
}

// Verify checks a compact-serialized token and returns its claims.
//
// The order is deliberate: nothing about what the claims mean is examined until the signature
// establishes that the issuer wrote them. A check performed before the signature is a check
// performed on attacker-supplied input.
func (v *Verifier) Verify(token string) (Claims, error) {
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		return Claims{}, fmt.Errorf("verify: %d segments, want 3: %w", len(segments), ErrMalformed)
	}

	rawHeader, err := decodeSegment(segments[0])
	if err != nil {
		return Claims{}, fmt.Errorf("verify: header: %w", ErrMalformed)
	}
	var head header
	if err := json.Unmarshal(rawHeader, &head); err != nil {
		return Claims{}, fmt.Errorf("verify: header is not an object: %w", ErrMalformed)
	}

	// The algorithm is compared against this verifier's configuration, never used to select a
	// verification path. That is the difference between an allowlist and algorithm confusion.
	if head.Algorithm != permittedAlgorithm {
		return Claims{}, fmt.Errorf("verify: alg %q is not %s: %w",
			head.Algorithm, permittedAlgorithm, ErrAlgorithm)
	}
	if head.Type != "" && !strings.EqualFold(head.Type, "JWT") && !strings.EqualFold(head.Type, "at+jwt") {
		return Claims{}, fmt.Errorf("verify: typ %q is not a JWT: %w", head.Type, ErrMalformed)
	}
	if head.KeyID == "" {
		return Claims{}, fmt.Errorf("verify: header carries no kid: %w", ErrMalformed)
	}

	signature, err := decodeSegment(segments[2])
	if err != nil {
		return Claims{}, fmt.Errorf("verify: signature: %w", ErrMalformed)
	}

	signed := segments[0] + "." + segments[1]
	if err := v.checkSignature(head.KeyID, signed, signature); err != nil {
		return Claims{}, err
	}

	rawPayload, err := decodeSegment(segments[1])
	if err != nil {
		return Claims{}, fmt.Errorf("verify: payload: %w", ErrMalformed)
	}

	claims, err := decodeClaims(rawPayload)
	if err != nil {
		return Claims{}, err
	}

	if err := v.checkRegistered(claims); err != nil {
		return Claims{}, err
	}

	if err := v.cfg.Requirement.Require(claims); err != nil {
		// The requirement's own message is wrapped rather than replaced, because it names
		// which claim was missing and that is what an operator needs. A requirement must not
		// include a claim value in its message; that obligation is the consumer's.
		return Claims{}, fmt.Errorf("%w: %w", ErrClaimRequirement, err)
	}

	return claims, nil
}

// checkSignature verifies the signature, refetching the key set once for an unknown identifier.
func (v *Verifier) checkSignature(kid, signed string, signature []byte) error {
	key, ok := v.cfg.Keys.Key(kid)
	if !ok {
		// An unknown identifier is most often a rotation this process has not seen yet, so one
		// refetch is attempted. It is rate limited by the source: the identifier is the one
		// input an attacker controls, and an unbounded refetch turns random identifiers into a
		// denial of service against the issuer.
		if err := v.cfg.Keys.Refetch(); err != nil {
			return fmt.Errorf("verify: kid %q unknown and the key set could not be reloaded: %w",
				kid, ErrKeysUnavailable)
		}
		key, ok = v.cfg.Keys.Key(kid)
	}
	if !ok {
		// Rejected rather than accepted. Failing open on an unknown key converts a
		// key-distribution problem into an authentication bypass.
		return fmt.Errorf("verify: kid %q: %w", kid, ErrUnknownKey)
	}

	digest := crypto.SHA256.New()
	digest.Write([]byte(signed))

	// Salt length equals the hash length, which RFC 7518 requires for PS256. PSSSaltLengthAuto
	// would accept a signature with any valid salt length, and accepting more than the
	// specification permits is how a verifier drifts from the profile it claims to implement.
	if err := rsa.VerifyPSS(key, crypto.SHA256, digest.Sum(nil), signature, &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
		Hash:       crypto.SHA256,
	}); err != nil {
		return fmt.Errorf("verify: %w", ErrSignature)
	}
	return nil
}

// checkRegistered validates issuer, audience, and the time bounds.
func (v *Verifier) checkRegistered(claims Claims) error {
	// Constant time is not required for a public identifier, and is used anyway: it costs
	// nothing here and removes the question of whether an early-exit comparison leaks
	// anything, which is a question nobody should have to re-answer while reading this.
	if subtle.ConstantTimeCompare([]byte(claims.Issuer), []byte(v.cfg.Issuer)) != 1 {
		return fmt.Errorf("verify: issuer %q: %w", claims.Issuer, ErrIssuer)
	}

	found := false
	for _, audience := range claims.Audience {
		if subtle.ConstantTimeCompare([]byte(audience), []byte(v.cfg.Audience)) == 1 {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("verify: audience %v does not contain %q: %w",
			claims.Audience, v.cfg.Audience, ErrAudience)
	}

	now := v.cfg.Now()

	if claims.ExpiresAt.IsZero() {
		// A token without an expiry never expires, which makes every revocation bound in
		// STD-IAM-002 §3.3 unenforceable. It is refused rather than given a default.
		return fmt.Errorf("verify: no exp claim: %w", ErrMalformed)
	}
	if now.After(claims.ExpiresAt.Add(v.cfg.MaxSkew)) {
		return fmt.Errorf("verify: expired at %s: %w", claims.ExpiresAt.UTC().Format(time.RFC3339), ErrExpired)
	}

	if !claims.IssuedAt.IsZero() && claims.IssuedAt.Add(-v.cfg.MaxSkew).After(now) {
		return fmt.Errorf("verify: issued at %s: %w", claims.IssuedAt.UTC().Format(time.RFC3339), ErrNotYetValid)
	}
	if !claims.NotBefore.IsZero() && claims.NotBefore.Add(-v.cfg.MaxSkew).After(now) {
		return fmt.Errorf("verify: not valid before %s: %w", claims.NotBefore.UTC().Format(time.RFC3339), ErrNotYetValid)
	}

	return nil
}

// decodeClaims parses the payload into typed registered claims plus the raw remainder.
func decodeClaims(payload []byte) (Claims, error) {
	var registered registeredClaims
	if err := json.Unmarshal(payload, &registered); err != nil {
		return Claims{}, fmt.Errorf("verify: payload is not an object: %w", ErrMalformed)
	}

	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return Claims{}, fmt.Errorf("verify: payload is not an object: %w", ErrMalformed)
	}

	audience, err := decodeAudience(raw["aud"])
	if err != nil {
		return Claims{}, err
	}

	claims := Claims{
		Issuer:   registered.Issuer,
		Subject:  registered.Subject,
		Audience: audience,
		raw:      raw,
	}
	if registered.ExpiresAt != nil {
		claims.ExpiresAt = time.Unix(*registered.ExpiresAt, 0)
	}
	if registered.IssuedAt != nil {
		claims.IssuedAt = time.Unix(*registered.IssuedAt, 0)
	}
	if registered.NotBefore != nil {
		claims.NotBefore = time.Unix(*registered.NotBefore, 0)
	}
	return claims, nil
}

// decodeAudience accepts the two shapes RFC 7519 permits.
func decodeAudience(encoded json.RawMessage) ([]string, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("verify: no aud claim: %w", ErrMalformed)
	}

	var single string
	if err := json.Unmarshal(encoded, &single); err == nil {
		return []string{single}, nil
	}

	var multiple []string
	if err := json.Unmarshal(encoded, &multiple); err == nil {
		return multiple, nil
	}

	return nil, fmt.Errorf("verify: aud is neither a string nor an array: %w", ErrMalformed)
}

// decodeSegment decodes one base64url segment.
//
// Unpadded strictly, which is what RFC 7515 requires. Accepting padded input would let two
// distinct encodings of one token both verify, and a token that has more than one valid
// serialization is a token a deduplication or replay check can be walked past.
func decodeSegment(segment string) ([]byte, error) {
	if strings.ContainsAny(segment, "+/=") {
		return nil, ErrMalformed
	}
	decoded, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		return nil, ErrMalformed
	}
	return decoded, nil
}
