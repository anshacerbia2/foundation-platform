package clientauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	// ErrRejected means the token endpoint refused the credential: a missing client, a key it does
	// not hold, an assertion it will not accept. A deployment defect, so it is not retried.
	ErrRejected = errors.New("clientauth: the token endpoint rejected the client credential")

	// ErrUnavailable means no token could be obtained for a reason a later attempt may not meet.
	ErrUnavailable = errors.New("clientauth: no access token could be obtained")
)

// Config is one client at one token endpoint.
type Config struct {
	// TokenURL is the token endpoint, at the address this process reaches it on.
	TokenURL string

	// Audience is the authorization server's issuer, which the assertion names (RFC 7523 §3). It is
	// configured rather than derived from TokenURL: a service reaches the kernel on an internal
	// address, and an assertion naming that address is refused.
	Audience string

	ClientID string
	Key      *Key

	// Client is the HTTP client the token request uses. Defaults to one with a 10 s timeout.
	Client *http.Client

	// Leeway is how long before its stated expiry a cached token is replaced. Default 30 s.
	Leeway time.Duration
}

// Tokens acquires and caches the client's access token.
type Tokens struct {
	cfg Config
	now func() time.Time

	mu     sync.Mutex
	token  string
	expiry time.Time
}

// NewTokens builds a token source for one client at one token endpoint.
func NewTokens(cfg Config) (*Tokens, error) {
	parsed, err := url.Parse(strings.TrimSpace(cfg.TokenURL))
	switch {
	case err != nil || parsed.Scheme == "" || parsed.Host == "":
		return nil, fmt.Errorf("clientauth: the token URL %q is not an absolute URL", cfg.TokenURL)
	case strings.TrimSpace(cfg.Audience) == "":
		return nil, errors.New("clientauth: the audience, the authorization server's issuer, is required")
	case strings.TrimSpace(cfg.ClientID) == "":
		return nil, errors.New("clientauth: a client_id is required")
	case cfg.Key == nil:
		return nil, errors.New("clientauth: a client key is required")
	case cfg.Leeway < 0:
		return nil, errors.New("clientauth: the leeway cannot be negative")
	}
	cfg.TokenURL = parsed.String()
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.Leeway == 0 {
		cfg.Leeway = 30 * time.Second
	}
	return &Tokens{cfg: cfg, now: time.Now}, nil
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

// Token returns a cached access token, or acquires one when none is held or the held one is within
// the leeway of its expiry.
//
// The token is held in memory only. It is never logged, never included in an error, and never
// written anywhere.
func (t *Tokens) Token(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	if t.token != "" && now.Add(t.cfg.Leeway).Before(t.expiry) {
		return t.token, nil
	}

	assertion, err := t.cfg.Key.Assertion(t.cfg.ClientID, t.cfg.Audience, now)
	if err != nil {
		return "", err
	}
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", t.cfg.ClientID)
	form.Set("client_assertion_type", AssertionType)
	form.Set("client_assertion", assertion)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.cfg.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("clientauth: building the token request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")

	response, err := t.cfg.Client.Do(request)
	if err != nil {
		return "", fmt.Errorf("clientauth: the token endpoint is unreachable: %w", ErrUnavailable)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("clientauth: the token response was truncated: %w", ErrUnavailable)
	}

	switch {
	case response.StatusCode == http.StatusBadRequest, response.StatusCode == http.StatusUnauthorized,
		response.StatusCode == http.StatusForbidden:
		// The body is discarded: an OAuth error response can echo the client identifier.
		return "", fmt.Errorf("clientauth: %s answered %d: %w", t.cfg.ClientID, response.StatusCode, ErrRejected)
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return "", fmt.Errorf("clientauth: the token endpoint answered %d: %w", response.StatusCode, ErrUnavailable)
	}

	var decoded tokenResponse
	if err := json.Unmarshal(body, &decoded); err != nil || decoded.AccessToken == "" {
		return "", fmt.Errorf("clientauth: the token response carried no access token: %w", ErrUnavailable)
	}
	if decoded.ExpiresIn <= 0 {
		// A token with no stated lifetime is used once rather than cached forever. Caching an
		// unknown lifetime is how a service presents an expired token and reads the resulting 401
		// as a permission problem.
		t.token, t.expiry = "", time.Time{}
		return decoded.AccessToken, nil
	}
	t.token = decoded.AccessToken
	t.expiry = now.Add(time.Duration(decoded.ExpiresIn) * time.Second)
	return t.token, nil
}

// Invalidate drops the cached token, so the next call acquires a fresh one. A resource that refused
// the token with 401 calls for it: a restarted kernel can refuse a token it issued before.
func (t *Tokens) Invalidate() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token, t.expiry = "", time.Time{}
}
