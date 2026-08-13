// Package redact removes credential-shaped values before they cross an operational
// boundary such as a log, problem document, or failure table.
package redact

import (
	"context"
	"log/slog"
	"regexp"
)

const Replacement = "[REDACTED]"

var patterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`),
	regexp.MustCompile(`(?i)((?:password|passwd|secret|token|api[_-]?key|authorization|cookie|credential|client[_-]?secret)\s*[=:]\s*)(?:"[^"]*"|'[^']*'|[^\s,;&]+)`),
	regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^\s:/@]+:)[^\s/@]+(@)`),
	regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`),
}

var sensitiveKey = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|authorization|cookie|credential|client[_-]?secret)`)

// String redacts credential-shaped values while preserving enough context to diagnose
// the failure that carried them.
func String(value string) string {
	for i, pattern := range patterns {
		switch i {
		case 2:
			value = pattern.ReplaceAllString(value, `${1}`+Replacement+`${2}`)
		case 3:
			value = pattern.ReplaceAllString(value, Replacement)
		default:
			value = pattern.ReplaceAllString(value, `${1}`+Replacement)
		}
	}
	return value
}

// SensitiveKey reports whether a structured field name is reserved for secret data.
func SensitiveKey(key string) bool {
	return sensitiveKey.MatchString(key)
}

// Handler redacts sensitive structured attributes and credential-shaped text before
// forwarding records to the wrapped slog handler.
type Handler struct {
	next           slog.Handler
	sensitiveGroup bool
}

// NewHandler wraps next. A nil handler is a programming error and panics immediately.
func NewHandler(next slog.Handler) *Handler {
	if next == nil {
		panic("redact: slog handler is required")
	}
	return &Handler{next: next}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	clean := slog.NewRecord(record.Time, record.Level, String(record.Message), record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		if h.sensitiveGroup {
			clean.AddAttrs(slog.String(attr.Key, Replacement))
		} else {
			clean.AddAttrs(redactAttr(attr))
		}
		return true
	})
	return h.next.Handle(ctx, clean)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, len(attrs))
	for i := range attrs {
		if h.sensitiveGroup {
			clean[i] = slog.String(attrs[i].Key, Replacement)
		} else {
			clean[i] = redactAttr(attrs[i])
		}
	}
	return &Handler{next: h.next.WithAttrs(clean), sensitiveGroup: h.sensitiveGroup}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{
		next:           h.next.WithGroup(name),
		sensitiveGroup: h.sensitiveGroup || SensitiveKey(name),
	}
}

func redactAttr(attr slog.Attr) slog.Attr {
	attr.Value = attr.Value.Resolve()
	if SensitiveKey(attr.Key) {
		return slog.String(attr.Key, Replacement)
	}

	switch attr.Value.Kind() {
	case slog.KindString:
		return slog.String(attr.Key, String(attr.Value.String()))
	case slog.KindGroup:
		group := attr.Value.Group()
		for i := range group {
			group[i] = redactAttr(group[i])
		}
		return slog.Group(attr.Key, attrsToAny(group)...)
	default:
		return attr
	}
}

func attrsToAny(attrs []slog.Attr) []any {
	values := make([]any, len(attrs))
	for i := range attrs {
		values[i] = attrs[i]
	}
	return values
}

var _ slog.Handler = (*Handler)(nil)
