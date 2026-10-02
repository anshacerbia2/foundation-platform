// Package clientauth gives a service an access token as its own workload: the OAuth 2.0 client
// credentials grant (RFC 6749 §4.4), authenticated with a signed JWT client assertion
// (private_key_jwt, RFC 7523). TDD-foundation-platform-002 §Workload Client Credentials.
//
// A workload holds a key pair, never a shared secret (STD-IAM-001 §3). This file is the key: read
// from its file, and the assertion signed with it.
package clientauth

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"
)

// MinKeyBits is the smallest RSA modulus a client key may have (STD-IAM-001 §3, the floor
// STD-IAM-002 §3.2.2 sets for signing keys).
const MinKeyBits = 3072

// AssertionType is the RFC 7523 §2.2 client assertion type.
const AssertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

// assertionLifetime bounds how long a signed assertion is accepted. The kernel refuses one
// presented twice, so the window only has to cover the request that carries it.
const assertionLifetime = time.Minute

// Key is a client's private key and the identifier its public half is registered under.
type Key struct {
	// ID is the key's RFC 7638 thumbprint, sent as the assertion's kid.
	ID      string
	private *rsa.PrivateKey
}

// LoadKey reads a client key from a PEM file: the form every deployable is given its key in.
func LoadKey(path string) (*Key, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		// The path is configuration and safe to name. The content is never read into a message.
		return nil, fmt.Errorf("clientauth: reading the client key at %s: %w", path, err)
	}
	return ParseKey(raw)
}

// ParseKey reads an RSA private key in PKCS#8 or PKCS#1 PEM and refuses one below MinKeyBits.
func ParseKey(pemBytes []byte) (*Key, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("clientauth: the client key is not PEM")
	}
	var private *rsa.PrivateKey
	switch block.Type {
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.New("clientauth: the client key is not a PKCS#8 key")
		}
		rsaKey, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("clientauth: the client key is not an RSA key")
		}
		private = rsaKey
	case "RSA PRIVATE KEY":
		parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.New("clientauth: the client key is not a PKCS#1 key")
		}
		private = parsed
	default:
		return nil, fmt.Errorf("clientauth: a %q PEM block is not a private key", block.Type)
	}
	return KeyFromRSA(private)
}

// KeyFromRSA wraps an RSA private key, refusing one below MinKeyBits.
func KeyFromRSA(private *rsa.PrivateKey) (*Key, error) {
	if private == nil {
		return nil, errors.New("clientauth: a private key is required")
	}
	if bits := private.N.BitLen(); bits < MinKeyBits {
		return nil, fmt.Errorf("clientauth: the client key is %d bits; at least %d are required", bits, MinKeyBits)
	}
	return &Key{ID: Thumbprint(&private.PublicKey), private: private}, nil
}

// Thumbprint is the RFC 7638 SHA-256 thumbprint of an RSA public key.
func Thumbprint(public *rsa.PublicKey) string {
	canonical := `{"e":"` + b64(big.NewInt(int64(public.E)).Bytes()) + `","kty":"RSA","n":"` + b64(public.N.Bytes()) + `"}`
	sum := sha256.Sum256([]byte(canonical))
	return b64(sum[:])
}

// Assertion signs an RFC 7523 client assertion: the client names itself as issuer and subject
// ("the subject MUST be the client_id of the OAuth client", §3), the authorization server's issuer
// is the audience, and a fresh jti makes it single-use.
func (k *Key) Assertion(clientID, audience string, now time.Time) (string, error) {
	switch {
	case strings.TrimSpace(clientID) == "":
		return "", errors.New("clientauth: a client_id is required")
	case strings.TrimSpace(audience) == "":
		return "", errors.New("clientauth: an audience is required")
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("clientauth: minting a jti: %w", err)
	}
	header, err := json.Marshal(map[string]string{"alg": "PS256", "typ": "JWT", "kid": k.ID})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"iss": clientID, "sub": clientID, "aud": audience, "jti": b64(nonce[:]),
		"iat": now.Unix(), "exp": now.Add(assertionLifetime).Unix(),
	})
	if err != nil {
		return "", err
	}
	input := b64(header) + "." + b64(claims)
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPSS(rand.Reader, k.private, crypto.SHA256, digest[:],
		&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	if err != nil {
		return "", fmt.Errorf("clientauth: signing the client assertion: %w", err)
	}
	return input + "." + b64(signature), nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
