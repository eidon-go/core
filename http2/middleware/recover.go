// Package middleware provides net/http middleware for servers built with
// http2 — or any other http.Handler. Every middleware has the transport-native
// shape func(http.Handler) http.Handler.
//
// Order: put Recover directly around the application handler, inside logging,
// auth and similar middleware, so that they observe the 500 it writes:
//
//	h := auth(logging(middleware.Recover(logger)(mux)))
//
// http2.Server adds tracing outside every middleware passed in this way.
package middleware

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
)

// Recover recovers a panic in next and logs it with the stack trace at error
// level. If the response has not been started yet, it answers 500 Internal
// Server Error. If the handler already wrote headers or body, a second
// response is impossible, so Recover aborts the response with
// http.ErrAbortHandler instead: net/http then drops the connection (HTTP/1)
// or resets the stream (HTTP/2), and the client sees an error rather than a
// truncated response that looks complete.
//
// http.ErrAbortHandler is re-raised: it is the documented way for a handler to
// abort the response, and net/http handles it silently.
//
// A nil logger means slog.Default().
func Recover(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tracked := &trackingWriter{ResponseWriter: w, wrote: false}

			defer func() {
				rec := recover()
				if rec == nil {
					return
				}

				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rec)
				}

				logger.ErrorContext(r.Context(), "http handler panic",
					slog.Any("panic", rec),
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Bool("response_started", tracked.wrote),
					slog.String("stack", string(debug.Stack())),
				)

				if tracked.wrote {
					panic(http.ErrAbortHandler)
				}

				http.Error(tracked, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}()

			next.ServeHTTP(tracked, r)
		})
	}
}

// trackingWriter records whether the response has been started.
//
// It forwards http.Flusher, http.Hijacker and io.ReaderFrom explicitly, not
// only through Unwrap: http.NewResponseController walks Unwrap, but some
// libraries type assert the writer directly (connect-go's server streams
// require http.Flusher, WebSocket libraries http.Hijacker, and io.Copy uses
// io.ReaderFrom for the sendfile fast path). Errors from the underlying
// writer are returned as they are, so that callers can compare them with
// net/http's sentinel errors.
type trackingWriter struct {
	http.ResponseWriter

	wrote bool
}

var (
	_ http.Flusher  = (*trackingWriter)(nil)
	_ http.Hijacker = (*trackingWriter)(nil)
	_ io.ReaderFrom = (*trackingWriter)(nil)
)

// WriteHeader records that the response has started, unless code is an
// informational (1xx) header such as 103 Early Hints, which may be followed by
// the real response. 101 Switching Protocols is final.
func (t *trackingWriter) WriteHeader(code int) {
	if code >= http.StatusOK || code == http.StatusSwitchingProtocols {
		t.wrote = true
	}

	t.ResponseWriter.WriteHeader(code)
}

func (t *trackingWriter) Write(b []byte) (int, error) {
	t.wrote = true

	return t.ResponseWriter.Write(b) //nolint:wrapcheck // callers compare net/http errors by identity
}

// ReadFrom copies src into the response, through the underlying writer's
// io.ReaderFrom when it has one.
func (t *trackingWriter) ReadFrom(src io.Reader) (int64, error) {
	t.wrote = true

	return io.Copy(t.ResponseWriter, src) //nolint:wrapcheck // as Write
}

// Unwrap lets http.NewResponseController reach the underlying writer.
func (t *trackingWriter) Unwrap() http.ResponseWriter { return t.ResponseWriter }

// Flush flushes the underlying writer when it supports flushing.
func (t *trackingWriter) Flush() {
	t.wrote = true
	_ = http.NewResponseController(t.ResponseWriter).Flush()
}

// Hijack hijacks the underlying connection when the writer supports it
// (HTTP/1.x); it returns an error wrapping http.ErrNotSupported otherwise.
func (t *trackingWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, buf, err := http.NewResponseController(t.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, fmt.Errorf("hijack: %w", err)
	}

	t.wrote = true

	return conn, buf, nil
}
