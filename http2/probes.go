package http2

import (
	"net/http"

	"github.com/eidon-go/core/lifecycle"
)

// Probe paths served by WithProbes.
const (
	// LivenessPath always answers 200 OK: the process is up and serving.
	// It deliberately checks no dependency (see lifecycle.Healthchecker).
	LivenessPath = "/livez"

	// ReadinessPath answers 200 OK when the readiness checker passed to
	// WithProbes reports ready, and 503 Service Unavailable otherwise.
	ReadinessPath = "/readyz"
)

// probes answers the probe paths and passes every other request to next.
// The response body never contains the readiness error: it may name
// internal hosts; app.App.HealthReport has the details.
func probes(next http.Handler, readiness lifecycle.Healthchecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case LivenessPath:
			writeProbe(w, http.StatusOK, "ok")
		case ReadinessPath:
			if readiness != nil && readiness.Healthcheck(r.Context()) != nil {
				writeProbe(w, http.StatusServiceUnavailable, "not ready")

				return
			}

			writeProbe(w, http.StatusOK, "ready")
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func writeProbe(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}
