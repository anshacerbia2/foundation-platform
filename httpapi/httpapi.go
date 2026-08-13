// Package httpapi builds the fixed inbound HTTP middleware chain and serializes every
// error as an RFC 7807 problem document.
package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/anshacerbia2/foundation-platform/observability"
)

const CorrelationHeader = "X-Correlation-ID"

type Middleware func(http.Handler) http.Handler

// Options supplies process-owned policy hooks. Authentication, authorization, and
// idempotency remain consumer decisions; Chain fixes where they run.
type Options struct {
	Telemetry      *observability.Telemetry
	Timeout        time.Duration
	MaxInFlight    int64
	RetryAfter     time.Duration
	Authentication Middleware
	Authorization  Middleware
	Idempotency    Middleware
}

// Chain builds recovery, correlation, logging, timeout/load-shedding, authentication,
// authorization, and idempotency in that fixed outer-to-inner order.
func Chain(opts Options) func(http.Handler) http.Handler {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = 256
	}
	if opts.RetryAfter <= 0 {
		opts.RetryAfter = time.Second
	}
	shedder := &loadShedder{max: opts.MaxInFlight, retryAfter: opts.RetryAfter, telemetry: opts.Telemetry}

	return func(next http.Handler) http.Handler {
		handler := next
		if opts.Idempotency != nil {
			handler = opts.Idempotency(handler)
		}
		if opts.Authorization != nil {
			handler = opts.Authorization(handler)
		}
		if opts.Authentication != nil {
			handler = opts.Authentication(handler)
		}
		handler = timeout(opts.Timeout, handler)
		handler = shedder.middleware(handler)
		handler = requestLog(opts.Telemetry, handler)
		handler = correlation(handler)
		handler = recovery(opts.Telemetry, handler)
		return handler
	}
}

func correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, correlationID, err := observability.EnsureCorrelation(r.Context(), r.Header.Get(CorrelationHeader))
		if err != nil {
			Problem(w, r, Internal, "Unable to create a request correlation identifier")
			return
		}
		*r = *r.WithContext(ctx)
		w.Header().Set(CorrelationHeader, correlationID.String())
		next.ServeHTTP(w, r)
	})
}

func timeout(duration time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), duration)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type loadShedder struct {
	max        int64
	retryAfter time.Duration
	telemetry  *observability.Telemetry
	inFlight   atomic.Int64
}

func (s *loadShedder) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := s.inFlight.Add(1)
		if current > s.max {
			s.inFlight.Add(-1)
			seconds := max(1, int64((s.retryAfter+time.Second-1)/time.Second))
			w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
			if s.telemetry != nil {
				s.telemetry.RecordShed(r.Context())
			}
			Problem(w, r, Overloaded, "Request capacity is temporarily exhausted")
			return
		}
		defer s.inFlight.Add(-1)
		next.ServeHTTP(w, r)
	})
}

func requestLog(telemetry *observability.Telemetry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		statusWriter := &statusResponseWriter{ResponseWriter: w}
		next.ServeHTTP(statusWriter, r)
		status := statusWriter.status
		if status == 0 {
			status = http.StatusOK
		}
		if telemetry != nil {
			duration := time.Since(started)
			telemetry.Logger(r.Context()).LogAttrs(r.Context(), slog.LevelInfo, "http request completed",
				slog.String("http.method", r.Method),
				slog.String("http.path", r.URL.Path),
				slog.Int("http.status", status),
				slog.Duration("duration", duration),
			)
			telemetry.RecordRequest(r.Context(), r.Method, status, duration)
		}
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func recovery(telemetry *observability.Telemetry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buffered := newBufferedResponse()
		defer func() {
			if recovered := recover(); recovered != nil {
				if telemetry != nil {
					telemetry.RecordPanic(r.Context())
					telemetry.Logger(r.Context()).ErrorContext(r.Context(), "panic recovered",
						slog.String("panic", fmt.Sprint(recovered)))
				}
				Problem(w, r, Internal, "The request could not be completed")
				return
			}
			buffered.commit(w)
		}()
		next.ServeHTTP(buffered, r)
	})
}

type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newBufferedResponse() *bufferedResponse {
	return &bufferedResponse{header: make(http.Header)}
}

func (w *bufferedResponse) Header() http.Header { return w.header }

func (w *bufferedResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *bufferedResponse) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(body)
}

func (w *bufferedResponse) commit(target http.ResponseWriter) {
	for key, values := range w.header {
		for _, value := range values {
			target.Header().Add(key, value)
		}
	}
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	target.WriteHeader(status)
	_, _ = target.Write(w.body.Bytes())
}

var _ http.ResponseWriter = (*bufferedResponse)(nil)
