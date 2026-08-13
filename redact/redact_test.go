package redact

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestStringRedactsCredentialShapes(t *testing.T) {
	for _, input := range []string{
		"authorization=Bearer abc.def.ghi",
		"password=hunter2 connection refused",
		"postgres://user:supersecret@db.example/control",
		"token=plain-token",
		`client_secret="two words" request failed`,
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.signature",
	} {
		got := String(input)
		if got == input || !strings.Contains(got, Replacement) {
			t.Errorf("String(%q) = %q, want a redacted value", input, got)
		}
		for _, secret := range []string{"hunter2", "supersecret", "plain-token", "two words", "abc.def.ghi"} {
			if strings.Contains(got, secret) {
				t.Errorf("String(%q) leaked %q in %q", input, secret, got)
			}
		}
	}
}

func TestHandlerRedactsMessagesAndAttributes(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&output, nil)))
	logger.LogAttrs(context.Background(), slog.LevelError,
		"publish failed token=visible",
		slog.String("authorization", "Bearer visible"),
		slog.Group("error", slog.String("detail", "password=visible")),
	)

	got := output.String()
	if strings.Contains(got, "visible") {
		t.Errorf("log contains credential material: %s", got)
	}
	if !strings.Contains(got, Replacement) {
		t.Errorf("log contains no redaction marker: %s", got)
	}
}

func TestHandlerRedactsEveryAttributeInsideASensitiveGroup(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&output, nil))).WithGroup("credentials")
	logger.Info("configured", slog.String("value", "visible"))
	if got := output.String(); strings.Contains(got, "visible") || !strings.Contains(got, Replacement) {
		t.Errorf("sensitive group was not redacted: %s", got)
	}
}

func TestStringPreservesOrdinaryFailureDetail(t *testing.T) {
	const input = "broker connection refused after 2 seconds"
	if got := String(input); got != input {
		t.Errorf("String(%q) = %q", input, got)
	}
}

func TestSensitiveKey(t *testing.T) {
	for _, key := range []string{"password", "access_token", "client-secret", "Cookie"} {
		if !SensitiveKey(key) {
			t.Errorf("SensitiveKey(%q) = false", key)
		}
	}
	for _, key := range []string{"event_id", "status", "attempts"} {
		if SensitiveKey(key) {
			t.Errorf("SensitiveKey(%q) = true", key)
		}
	}
}
