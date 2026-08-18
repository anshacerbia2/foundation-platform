package verify_test

// Every test here is a token that must be rejected, or the one shape that must be accepted.
// A verifier is only as good as the set of forgeries it refuses, so the suite is written as that
// set: wrong algorithm, no algorithm, a symmetric algorithm, a token nominating its own key, a
// key too short, an unknown key, a swapped signature, a wrong issuer, a wrong audience, an
// expired token, a token with no expiry, and a padded re-encoding of a valid token.

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/verify"
)

const (
	testIssuer   = "https://identity.scnehaux.com/realms/scnehaux"
	testAudience = "identity-control"
	testKeyID    = "key-2026-08"
)

var (
	// Generated once for the package. A 3072-bit key is the profile minimum and generating one
	// per test would dominate the suite's runtime without testing anything further.
	signingKey  *rsa.PrivateKey
	fixedNow    = time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	requirement = verify.RequirementFunc(func(claims verify.Claims) error {
		// This is the consumer's rule, standing in for the one identity-control supplies. The
		// package under test never names this claim.
		if _, ok := claims.String("principal_id"); !ok {
			return errors.New("principal_id is absent")
		}
		return nil
	})
)

func TestMain(m *testing.M) {
	var err error
	signingKey, err = rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		panic(err)
	}
	m.Run()
}

func keys() verify.StaticKeys {
	return verify.StaticKeys{testKeyID: &signingKey.PublicKey}
}

func newVerifier(t *testing.T, source verify.KeySource) *verify.Verifier {
	t.Helper()
	v, err := verify.New(verify.Config{
		Issuer:      testIssuer,
		Audience:    testAudience,
		Keys:        source,
		Requirement: requirement,
		MaxSkew:     30 * time.Second,
		Now:         func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return v
}

// sign builds a compact token. The header and payload are supplied as maps so a test can produce
// a shape a well-behaved issuer never would.
func sign(t *testing.T, key *rsa.PrivateKey, head, payload map[string]any) string {
	t.Helper()

	encode := func(value map[string]any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}

	signed := encode(head) + "." + encode(payload)

	digest := crypto.SHA256.New()
	digest.Write([]byte(signed))
	signature, err := rsa.SignPSS(rand.Reader, key, crypto.SHA256, digest.Sum(nil), &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
		Hash:       crypto.SHA256,
	})
	if err != nil {
		t.Fatalf("SignPSS: %v", err)
	}
	return signed + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func validHeader() map[string]any {
	return map[string]any{"alg": "PS256", "kid": testKeyID, "typ": "JWT"}
}

func validPayload() map[string]any {
	return map[string]any{
		"iss":          testIssuer,
		"sub":          "protocol-subject",
		"aud":          []string{testAudience},
		"iat":          fixedNow.Add(-time.Minute).Unix(),
		"exp":          fixedNow.Add(14 * time.Minute).Unix(),
		"principal_id": "019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71",
	}
}

func TestValidTokenIsAccepted(t *testing.T) {
	token := sign(t, signingKey, validHeader(), validPayload())

	claims, err := newVerifier(t, keys()).Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.Issuer != testIssuer {
		t.Errorf("Issuer = %q", claims.Issuer)
	}
	if claims.Subject != "protocol-subject" {
		t.Errorf("Subject = %q", claims.Subject)
	}
	principal, ok := claims.String("principal_id")
	if !ok || principal != "019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71" {
		t.Errorf("principal_id = %q, present = %v", principal, ok)
	}
}

// TestAlgorithmIsTakenFromConfigurationNotTheToken is the algorithm-confusion assertion. A
// verifier that selected its verification path from the header would accept the first three
// cases, and each is a complete bypass.
func TestAlgorithmIsTakenFromConfigurationNotTheToken(t *testing.T) {
	for _, alg := range []string{"none", "None", "HS256", "RS256", "ES256", "PS384", ""} {
		t.Run("alg "+alg, func(t *testing.T) {
			head := validHeader()
			head["alg"] = alg
			token := sign(t, signingKey, head, validPayload())

			_, err := newVerifier(t, keys()).Verify(token)
			if !errors.Is(err, verify.ErrAlgorithm) {
				t.Fatalf("error = %v, want ErrAlgorithm", err)
			}
		})
	}
}

// TestTokenCannotNominateItsOwnKey is the assertion that matters most in this file.
//
// The token carries jku, x5u, jwk, and x5c pointing at an attacker's key, and is signed by that
// key. If any of them were honoured the signature would verify, and the signature would have
// stopped being a control at all.
func TestTokenCannotNominateItsOwnKey(t *testing.T) {
	attacker, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	head := validHeader()
	head["jku"] = "https://attacker.test/jwks.json"
	head["x5u"] = "https://attacker.test/chain.pem"
	head["jwk"] = map[string]any{
		"kty": "RSA",
		"kid": testKeyID,
		"n":   base64.RawURLEncoding.EncodeToString(attacker.PublicKey.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1}),
	}
	head["x5c"] = []string{"MIIB-not-a-real-certificate"}

	token := sign(t, attacker, head, validPayload())

	_, err = newVerifier(t, keys()).Verify(token)
	if !errors.Is(err, verify.ErrSignature) {
		t.Fatalf("error = %v, want ErrSignature: a token must not choose the key that validates it", err)
	}
}

func TestSignatureFromAnotherKeyIsRejected(t *testing.T) {
	other, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	token := sign(t, other, validHeader(), validPayload())

	if _, err := newVerifier(t, keys()).Verify(token); !errors.Is(err, verify.ErrSignature) {
		t.Fatalf("error = %v, want ErrSignature", err)
	}
}

// TestTamperedPayloadIsRejected takes a valid token and swaps its payload for one claiming a
// different subject, keeping the original signature.
func TestTamperedPayloadIsRejected(t *testing.T) {
	token := sign(t, signingKey, validHeader(), validPayload())
	segments := strings.Split(token, ".")

	forged := validPayload()
	forged["principal_id"] = "019235f1-0000-7000-8000-000000000000"
	raw, err := json.Marshal(forged)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	segments[1] = base64.RawURLEncoding.EncodeToString(raw)

	_, err = newVerifier(t, keys()).Verify(strings.Join(segments, "."))
	if !errors.Is(err, verify.ErrSignature) {
		t.Fatalf("error = %v, want ErrSignature", err)
	}
}

// TestUnknownKeyIsRejectedAfterOneRefetch asserts the fail-closed branch. Failing open on an
// unknown identifier converts a key-distribution problem into an authentication bypass.
func TestUnknownKeyIsRejectedAfterOneRefetch(t *testing.T) {
	source := &countingSource{keys: verify.StaticKeys{}}

	head := validHeader()
	head["kid"] = "a-key-nobody-published"
	token := sign(t, signingKey, head, validPayload())

	_, err := newVerifier(t, source).Verify(token)
	if !errors.Is(err, verify.ErrUnknownKey) {
		t.Fatalf("error = %v, want ErrUnknownKey", err)
	}
	if source.refetches != 1 {
		t.Errorf("refetched %d times, want exactly 1", source.refetches)
	}
}

// TestRotationIsResolvedByOneRefetch is the other side: a key the process has not seen yet is
// adopted, so a rotation does not reject valid tokens.
func TestRotationIsResolvedByOneRefetch(t *testing.T) {
	source := &countingSource{keys: verify.StaticKeys{}, onRefetch: func(s *countingSource) {
		s.keys[testKeyID] = &signingKey.PublicKey
	}}

	token := sign(t, signingKey, validHeader(), validPayload())

	if _, err := newVerifier(t, source).Verify(token); err != nil {
		t.Fatalf("Verify after rotation: %v", err)
	}
	if source.refetches != 1 {
		t.Errorf("refetched %d times, want 1", source.refetches)
	}
}

func TestUnavailableKeySourceIsDistinctFromAnUnknownKey(t *testing.T) {
	source := &countingSource{keys: verify.StaticKeys{}, refetchErr: errors.New("issuer unreachable")}

	token := sign(t, signingKey, validHeader(), validPayload())

	_, err := newVerifier(t, source).Verify(token)
	if !errors.Is(err, verify.ErrKeysUnavailable) {
		t.Fatalf("error = %v, want ErrKeysUnavailable", err)
	}
	if errors.Is(err, verify.ErrUnknownKey) {
		t.Error("an unreachable issuer is reported as an unknown key; the two need different responses")
	}
}

func TestMissingKeyIDIsRejected(t *testing.T) {
	head := validHeader()
	delete(head, "kid")
	token := sign(t, signingKey, head, validPayload())

	if _, err := newVerifier(t, keys()).Verify(token); !errors.Is(err, verify.ErrMalformed) {
		t.Fatalf("error = %v, want ErrMalformed", err)
	}
}

func TestIssuerIsComparedExactly(t *testing.T) {
	for _, issuer := range []string{
		testIssuer + ".attacker.test",
		"https://attacker.test/" + testIssuer,
		strings.ToUpper(testIssuer),
		testIssuer + "/",
		"",
	} {
		t.Run(issuer, func(t *testing.T) {
			payload := validPayload()
			payload["iss"] = issuer
			token := sign(t, signingKey, validHeader(), payload)

			if _, err := newVerifier(t, keys()).Verify(token); !errors.Is(err, verify.ErrIssuer) {
				t.Fatalf("error = %v, want ErrIssuer", err)
			}
		})
	}
}

func TestAudienceMustContainThisResource(t *testing.T) {
	cases := map[string]any{
		"another resource":   []string{"organization-control"},
		"empty array":        []string{},
		"prefix":             []string{testAudience + "-staging"},
		"single wrong value": "organization-control",
	}
	for name, audience := range cases {
		t.Run(name, func(t *testing.T) {
			payload := validPayload()
			payload["aud"] = audience
			token := sign(t, signingKey, validHeader(), payload)

			if _, err := newVerifier(t, keys()).Verify(token); !errors.Is(err, verify.ErrAudience) {
				t.Fatalf("error = %v, want ErrAudience", err)
			}
		})
	}
}

// TestAudienceAcceptsBothShapes covers what RFC 7519 permits, so a conformant issuer is not
// rejected for choosing the string form.
func TestAudienceAcceptsBothShapes(t *testing.T) {
	for name, audience := range map[string]any{
		"string": testAudience,
		"array":  []string{"other", testAudience},
	} {
		t.Run(name, func(t *testing.T) {
			payload := validPayload()
			payload["aud"] = audience
			token := sign(t, signingKey, validHeader(), payload)

			if _, err := newVerifier(t, keys()).Verify(token); err != nil {
				t.Fatalf("Verify: %v", err)
			}
		})
	}
}

func TestExpiryIsEnforcedWithBoundedSkew(t *testing.T) {
	cases := map[string]struct {
		exp     time.Time
		wantErr error
	}{
		"inside the window":            {fixedNow.Add(time.Minute), nil},
		"just expired but within skew": {fixedNow.Add(-20 * time.Second), nil},
		"expired beyond skew":          {fixedNow.Add(-2 * time.Minute), verify.ErrExpired},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			payload := validPayload()
			payload["exp"] = tc.exp.Unix()
			token := sign(t, signingKey, validHeader(), payload)

			_, err := newVerifier(t, keys()).Verify(token)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestATokenWithoutAnExpiryIsRefused states the property directly: a token that never expires
// makes every revocation bound in STD-IAM-002 §3.3 unenforceable.
func TestATokenWithoutAnExpiryIsRefused(t *testing.T) {
	payload := validPayload()
	delete(payload, "exp")
	token := sign(t, signingKey, validHeader(), payload)

	if _, err := newVerifier(t, keys()).Verify(token); !errors.Is(err, verify.ErrMalformed) {
		t.Fatalf("error = %v, want ErrMalformed", err)
	}
}

func TestFutureTokenIsRejected(t *testing.T) {
	payload := validPayload()
	payload["nbf"] = fixedNow.Add(10 * time.Minute).Unix()
	token := sign(t, signingKey, validHeader(), payload)

	if _, err := newVerifier(t, keys()).Verify(token); !errors.Is(err, verify.ErrNotYetValid) {
		t.Fatalf("error = %v, want ErrNotYetValid", err)
	}
}

// TestNonRawURLEncodingIsRejected keeps a token from having more than one valid serialization.
// Two encodings of one token is one a replay or deduplication check can be walked past.
//
// The property is asserted per character class rather than by re-encoding a real payload: whether
// standard encoding actually differs from raw encoding depends on whether the payload length
// happens to be a multiple of three, so a test built that way passes or fails on the size of the
// claim set rather than on the verifier.
func TestNonRawURLEncodingIsRejected(t *testing.T) {
	token := sign(t, signingKey, validHeader(), validPayload())
	segments := strings.Split(token, ".")

	// Padding, and the two characters standard base64 uses where base64url uses - and _.
	for _, suffix := range []string{"=", "==", "+", "/"} {
		t.Run("payload with "+suffix, func(t *testing.T) {
			mutated := []string{segments[0], segments[1] + suffix, segments[2]}
			if _, err := newVerifier(t, keys()).Verify(strings.Join(mutated, ".")); err == nil {
				t.Fatalf("a segment containing %q was accepted", suffix)
			}
		})
		t.Run("header with "+suffix, func(t *testing.T) {
			mutated := []string{segments[0] + suffix, segments[1], segments[2]}
			if _, err := newVerifier(t, keys()).Verify(strings.Join(mutated, ".")); err == nil {
				t.Fatalf("a segment containing %q was accepted", suffix)
			}
		})
	}
}

func TestMalformedShapesAreRejected(t *testing.T) {
	valid := sign(t, signingKey, validHeader(), validPayload())
	segments := strings.Split(valid, ".")

	cases := map[string]string{
		"empty":                   "",
		"one segment":             segments[0],
		"two segments":            segments[0] + "." + segments[1],
		"four segments":           valid + ".extra",
		"header not json":         base64.RawURLEncoding.EncodeToString([]byte("not json")) + "." + segments[1] + "." + segments[2],
		"payload not json":        segments[0] + "." + base64.RawURLEncoding.EncodeToString([]byte("not json")) + "." + segments[2],
		"signature not base64url": segments[0] + "." + segments[1] + ".!!!!",
	}

	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := newVerifier(t, keys()).Verify(token); err == nil {
				t.Fatal("a malformed token was accepted")
			}
		})
	}
}

// TestClaimRequirementRunsLast asserts the ordering. A requirement evaluated before the
// signature would be reading attacker-supplied input.
func TestClaimRequirementRunsLast(t *testing.T) {
	var ran bool
	v, err := verify.New(verify.Config{
		Issuer: testIssuer, Audience: testAudience, Keys: keys(),
		Now: func() time.Time { return fixedNow },
		Requirement: verify.RequirementFunc(func(verify.Claims) error {
			ran = true
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// A token with a bad signature must not reach the requirement.
	other, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := v.Verify(sign(t, other, validHeader(), validPayload())); err == nil {
		t.Fatal("a forged token was accepted")
	}
	if ran {
		t.Error("the claim requirement ran against an unverified token")
	}

	if _, err := v.Verify(sign(t, signingKey, validHeader(), validPayload())); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !ran {
		t.Error("the claim requirement did not run for a valid token")
	}
}

func TestClaimRequirementFailureIsReported(t *testing.T) {
	payload := validPayload()
	delete(payload, "principal_id")
	token := sign(t, signingKey, validHeader(), payload)

	_, err := newVerifier(t, keys()).Verify(token)
	if !errors.Is(err, verify.ErrClaimRequirement) {
		t.Fatalf("error = %v, want ErrClaimRequirement", err)
	}
	// The requirement's own message survives, because it names which claim was missing.
	if !strings.Contains(err.Error(), "principal_id") {
		t.Errorf("error does not name the missing claim: %v", err)
	}
}

// TestConfigRefusesAVerifierWithoutARequirement is the guard against a verifier that checks the
// mechanics and calls itself compliant. Without a requirement, STD-IAM-002 §3.5 is not met.
func TestConfigRefusesAVerifierWithoutARequirement(t *testing.T) {
	_, err := verify.New(verify.Config{
		Issuer: testIssuer, Audience: testAudience, Keys: keys(),
	})
	if err == nil {
		t.Fatal("New accepted a verifier with no claim requirement")
	}
	if !strings.Contains(err.Error(), "ClaimRequirement") {
		t.Errorf("the error does not name what is missing: %v", err)
	}
}

func TestConfigRefusesSkewAboveTheCeiling(t *testing.T) {
	_, err := verify.New(verify.Config{
		Issuer: testIssuer, Audience: testAudience, Keys: keys(),
		Requirement: requirement, MaxSkew: 5 * time.Minute,
	})
	if err == nil {
		t.Fatal("New accepted a skew above the 60s ceiling")
	}
}

func TestConfigRefusesMissingDependencies(t *testing.T) {
	base := verify.Config{
		Issuer: testIssuer, Audience: testAudience, Keys: keys(), Requirement: requirement,
	}
	cases := map[string]func(verify.Config) verify.Config{
		"no issuer":   func(c verify.Config) verify.Config { c.Issuer = ""; return c },
		"no audience": func(c verify.Config) verify.Config { c.Audience = ""; return c },
		"no keys":     func(c verify.Config) verify.Config { c.Keys = nil; return c },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := verify.New(mutate(base)); err == nil {
				t.Fatal("New accepted an incomplete configuration")
			}
		})
	}
}

func TestClaimAccessors(t *testing.T) {
	payload := validPayload()
	payload["membership_version"] = 14
	payload["nested"] = map[string]any{"a": 1}
	token := sign(t, signingKey, validHeader(), payload)

	claims, err := newVerifier(t, keys()).Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if version, ok := claims.Int64("membership_version"); !ok || version != 14 {
		t.Errorf("Int64 = %d, present = %v", version, ok)
	}
	if _, ok := claims.Raw("nested"); !ok {
		t.Error("Raw did not return a nested claim")
	}
	if !claims.Has("principal_id") {
		t.Error("Has reported a present claim as absent")
	}
	if claims.Has("absent") {
		t.Error("Has reported an absent claim as present")
	}
	if _, ok := claims.String("membership_version"); ok {
		t.Error("String returned a value for a numeric claim")
	}
}

// countingSource records refetches so the rate-limit and fail-closed behaviour is observable.
type countingSource struct {
	keys       verify.StaticKeys
	refetches  int
	refetchErr error
	onRefetch  func(*countingSource)
}

func (s *countingSource) Key(kid string) (*rsa.PublicKey, bool) {
	key, ok := s.keys[kid]
	return key, ok
}

func (s *countingSource) Refetch() error {
	s.refetches++
	if s.refetchErr != nil {
		return s.refetchErr
	}
	if s.onRefetch != nil {
		s.onRefetch(s)
	}
	return nil
}
