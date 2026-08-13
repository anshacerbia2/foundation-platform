package httpapi

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestNewServerAppliesTransportDefaults(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server, err := NewServer(":8080", handler, ServerConfig{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if server.ReadTimeout != 10*time.Second || server.WriteTimeout != 30*time.Second {
		t.Errorf("server timeouts = read %v, write %v", server.ReadTimeout, server.WriteTimeout)
	}
	if server.Handler == nil || server.MaxHeaderBytes == 0 {
		t.Error("server omitted its handler or header bound")
	}
}

func TestNewServerRequiresAHandler(t *testing.T) {
	if _, err := NewServer(":8080", nil, ServerConfig{}); err == nil {
		t.Error("NewServer accepted a nil handler")
	}
}

func TestShutdownValidatesItsBoundary(t *testing.T) {
	if err := Shutdown(context.Background(), nil, time.Second); err == nil {
		t.Error("Shutdown accepted a nil server")
	}
	if err := Shutdown(context.Background(), &http.Server{}, 0); err == nil {
		t.Error("Shutdown accepted an absent grace period")
	}
}
