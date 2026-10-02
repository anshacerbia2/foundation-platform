package clientauth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	keyOnce sync.Once
	testKey *rsa.PrivateKey
)

// rsaKey is one 3072-bit key for the suite: generating it is the slow part.
func rsaKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	keyOnce.Do(func() {
		k, err := rsa.GenerateKey(rand.Reader, MinKeyBits)
		if err != nil {
			panic(err)
		}
		testKey = k
	})
	return testKey
}

func clientKey(t *testing.T) *Key {
	t.Helper()
	key, err := KeyFromRSA(rsaKey(t))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestAKeyIsReadFromPEMAndASmallOneIsRefused(t *testing.T) {
	dir := t.TempDir()
	for name, block := range map[string]*pem.Block{
		"pkcs1.pem": {Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rsaKey(t))},
		"pkcs8.pem": func() *pem.Block {
			der, err := x509.MarshalPKCS8PrivateKey(rsaKey(t))
			if err != nil {
				t.Fatal(err)
			}
			return &pem.Block{Type: "PRIVATE KEY", Bytes: der}
		}(),
	} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
		key, err := LoadKey(path)
		if err != nil || key.ID != Thumbprint(&rsaKey(t).PublicKey) {
			t.Errorf("%s: %v, %v", name, key, err)
		}
	}
	small, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := KeyFromRSA(small); err == nil {
		t.Error("a 2048-bit key was accepted")
	}
	if _, err := ParseKey([]byte("not pem")); err == nil {
		t.Error("a non-PEM key was accepted")
	}
}

// The assertion RFC 7523 §3 describes: the client as issuer and subject, the issuer as audience, a
// fresh jti, a minute's life, PS256 under the key's thumbprint.
func TestTheAssertionIsAnRFC7523ClientAssertion(t *testing.T) {
	key := clientKey(t)
	now := time.Unix(1_800_000_000, 0)
	first, err := key.Assertion("organization-control", "https://idp.example/realms/scnehaux", now)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := key.Assertion("organization-control", "https://idp.example/realms/scnehaux", now)

	parts := strings.Split(first, ".")
	if len(parts) != 3 {
		t.Fatalf("the assertion has %d parts", len(parts))
	}
	var header map[string]string
	var claims map[string]any
	decode := func(s string, into any) {
		raw, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, into); err != nil {
			t.Fatal(err)
		}
	}
	decode(parts[0], &header)
	decode(parts[1], &claims)
	if header["alg"] != "PS256" || header["kid"] != key.ID {
		t.Errorf("header %v", header)
	}
	if claims["iss"] != "organization-control" || claims["sub"] != "organization-control" ||
		claims["aud"] != "https://idp.example/realms/scnehaux" ||
		claims["exp"].(float64)-claims["iat"].(float64) != 60 {
		t.Errorf("claims %v", claims)
	}
	var other map[string]any
	decode(strings.Split(second, ".")[1], &other)
	if other["jti"] == claims["jti"] {
		t.Error("two assertions share a jti")
	}
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPSS(&rsaKey(t).PublicKey, crypto.SHA256, digest[:], signature,
		&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash}); err != nil {
		t.Errorf("the signature does not verify: %v", err)
	}
	if _, err := key.Assertion("", "aud", now); err == nil {
		t.Error("an assertion without a client_id was signed")
	}
}

func tokenServer(t *testing.T, handler http.HandlerFunc) (*Tokens, *int32) {
	t.Helper()
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("client_id") != "organization-control" ||
			r.Form.Get("client_assertion_type") != AssertionType || r.Form.Get("client_assertion") == "" {
			t.Errorf("token request form %v", r.Form)
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	tokens, err := NewTokens(Config{
		TokenURL: server.URL + "/token", Audience: "https://idp.example/realms/scnehaux",
		ClientID: "organization-control", Key: clientKey(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	return tokens, &calls
}

func TestATokenIsCachedUntilTheLeewayAndInvalidatedOnRequest(t *testing.T) {
	tokens, calls := tokenServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"token","expires_in":300}`))
	})
	now := time.Unix(1_800_000_000, 0)
	tokens.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		if token, err := tokens.Token(context.Background()); err != nil || token != "token" {
			t.Fatalf("Token: %q, %v", token, err)
		}
	}
	if *calls != 1 {
		t.Errorf("%d token requests for three calls, want one", *calls)
	}
	now = now.Add(271 * time.Second) // within the 30 s leeway of the 300 s expiry
	if _, err := tokens.Token(context.Background()); err != nil || *calls != 2 {
		t.Errorf("a token within its leeway was reused: %d calls, %v", *calls, err)
	}
	tokens.Invalidate()
	if _, err := tokens.Token(context.Background()); err != nil || *calls != 3 {
		t.Errorf("an invalidated token was reused: %d calls", *calls)
	}
}

func TestARefusedCredentialIsRejectedAndAnOutageIsUnavailable(t *testing.T) {
	for status, want := range map[int]error{
		http.StatusUnauthorized:        ErrRejected,
		http.StatusBadRequest:          ErrRejected,
		http.StatusServiceUnavailable:  ErrUnavailable,
		http.StatusInternalServerError: ErrUnavailable,
	} {
		tokens, _ := tokenServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"organization-control"}`))
		})
		if _, err := tokens.Token(context.Background()); !errors.Is(err, want) {
			t.Errorf("%d answered %v, want %v", status, err, want)
		}
	}
}

func TestATokenWithNoLifetimeIsUsedOnce(t *testing.T) {
	tokens, calls := tokenServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"token"}`))
	})
	_, _ = tokens.Token(context.Background())
	_, _ = tokens.Token(context.Background())
	if *calls != 2 {
		t.Errorf("a token with no expires_in was cached: %d requests", *calls)
	}
}

func TestATokenSourceRefusesAnIncompleteConfiguration(t *testing.T) {
	key := clientKey(t)
	for name, cfg := range map[string]Config{
		"relative URL": {TokenURL: "/token", Audience: "a", ClientID: "c", Key: key},
		"no audience":  {TokenURL: "https://idp/token", ClientID: "c", Key: key},
		"no client":    {TokenURL: "https://idp/token", Audience: "a", Key: key},
		"no key":       {TokenURL: "https://idp/token", Audience: "a", ClientID: "c"},
	} {
		if _, err := NewTokens(cfg); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}
