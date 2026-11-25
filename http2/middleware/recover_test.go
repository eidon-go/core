package middleware_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/http2/middleware"
)

func TestRecover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus int
		wantBody   string
		wantLog    bool
		wantAbort  bool // the response had started: Recover aborts it
	}{
		{
			name:       "no panic",
			handler:    func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") },
			wantStatus: http.StatusOK,
			wantBody:   "ok",
		},
		{
			name:       "panic before the response",
			handler:    func(http.ResponseWriter, *http.Request) { panic("boom") },
			wantStatus: http.StatusInternalServerError,
			wantBody:   "Internal Server Error\n",
			wantLog:    true,
		},
		{
			name: "panic after the response started",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(w, "partial")

				panic("boom")
			},
			wantStatus: http.StatusAccepted,
			wantBody:   "partial",
			wantLog:    true,
			wantAbort:  true,
		},
		{
			name: "panic after ReadFrom",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				rf, _ := w.(io.ReaderFrom)
				_, _ = rf.ReadFrom(strings.NewReader("copied"))

				panic("boom")
			},
			wantStatus: http.StatusOK,
			wantBody:   "copied",
			wantLog:    true,
			wantAbort:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs bytes.Buffer

			logger := slog.New(slog.NewTextHandler(&logs, nil))
			rec := httptest.NewRecorder()
			serve := func() {
				middleware.Recover(logger)(tt.handler).ServeHTTP(rec, httptest.NewRequestWithContext(
					t.Context(), http.MethodGet, "/x", nil))
			}

			if tt.wantAbort {
				assert.PanicsWithValue(t, http.ErrAbortHandler, serve)
			} else {
				assert.NotPanics(t, serve)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantBody, rec.Body.String())

			if tt.wantLog {
				assert.Contains(t, logs.String(), "http handler panic")
				assert.Contains(t, logs.String(), "panic=boom")
				assert.Contains(t, logs.String(), "path=/x")
			} else {
				assert.Empty(t, logs.String())
			}
		})
	}
}

func TestRecover_ReraisesErrAbortHandler(t *testing.T) {
	t.Parallel()

	h := middleware.Recover(nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	assert.PanicsWithValue(t, http.ErrAbortHandler, func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	})
}

// An informational header (103 Early Hints) does not start the response: a
// panic after it still gets a 500. httptest.ResponseRecorder would keep 103 as
// the final code, so this runs against a real server.
func TestRecover_PanicAfterInformationalHeader(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(middleware.Recover(slog.New(slog.DiscardHandler))(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Link", "</style.css>; rel=preload")
			w.WriteHeader(http.StatusEarlyHints)
			panic("boom")
		})))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// A panic after part of the body was sent must not look like a complete 200
// to the client: the response is aborted and reading it fails.
func TestRecover_AbortsAStartedResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(middleware.Recover(slog.New(slog.DiscardHandler))(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"items":[1,2,`)
			_ = http.NewResponseController(w).Flush()

			panic("boom")
		})))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err, "headers were sent before the panic")

	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err, "the truncated body is reported as an error")
}

// failingWriter fails every Write with a net/http sentinel error.
type failingWriter struct{ *httptest.ResponseRecorder }

func (failingWriter) Write([]byte) (int, error) { return 0, http.ErrBodyNotAllowed }

func TestRecover_WriteErrorsAreNotWrapped(t *testing.T) {
	t.Parallel()

	var writeErr error

	h := middleware.Recover(nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, writeErr = w.Write([]byte("x"))
	}))

	h.ServeHTTP(failingWriter{httptest.NewRecorder()}, httptest.NewRequestWithContext(
		t.Context(), http.MethodHead, "/", nil))

	assert.Same(t, http.ErrBodyNotAllowed, writeErr, "callers compare net/http errors by identity")
}

func TestRecover_ForwardsWriterInterfaces(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()

	var (
		isFlusher, isHijacker bool
		hijackErr, flushErr   error
	)

	h := middleware.Recover(nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var flusher http.Flusher
		if flusher, isFlusher = w.(http.Flusher); isFlusher {
			flusher.Flush()
		}

		var hijacker http.Hijacker
		if hijacker, isHijacker = w.(http.Hijacker); isHijacker {
			_, _, hijackErr = hijacker.Hijack()
		}

		flushErr = http.NewResponseController(w).Flush()
	}))

	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))

	require.True(t, isFlusher, "http.Flusher on a bare type assertion")
	require.True(t, isHijacker, "http.Hijacker on a bare type assertion")
	require.ErrorIs(t, hijackErr, http.ErrNotSupported, "the recorder cannot be hijacked")
	require.NoError(t, flushErr, "Unwrap reaches the recorder")
	assert.True(t, rec.Flushed)
}
