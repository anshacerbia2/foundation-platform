package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// ServerConfig defines transport deadlines. Configuration is injected by the
// composition root; this package reads no environment variable.
type ServerConfig struct {
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
}

func (c *ServerConfig) applyDefaults() {
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 10 * time.Second
	}
	if c.ReadHeaderTimeout <= 0 {
		c.ReadHeaderTimeout = 5 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 60 * time.Second
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = 1 << 20
	}
}

// NewServer constructs an HTTP server and starts nothing.
func NewServer(address string, handler http.Handler, cfg ServerConfig) (*http.Server, error) {
	if handler == nil {
		return nil, errors.New("httpapi: handler is required")
	}
	cfg.applyDefaults()
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}, nil
}

// Shutdown drains a server within grace or an earlier parent deadline.
func Shutdown(ctx context.Context, server *http.Server, grace time.Duration) error {
	if server == nil {
		return errors.New("httpapi: server is required")
	}
	if grace <= 0 {
		return errors.New("httpapi: shutdown grace must be positive")
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
