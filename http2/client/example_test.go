package client_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/eidon-go/core/http2/client"
)

// bearer adds an Authorization header to every request.
func bearer(token string) func(http.RoundTripper) http.RoundTripper {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			req = req.Clone(req.Context())
			req.Header.Set("Authorization", "Bearer "+token)

			return next.RoundTrip(req)
		})
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A client for a slow third-party API: a longer wait for the first byte, an
// auth header on every request, and a per-call deadline from the context. It
// calls a remote API, so it has no output to check.
func Example() { //nolint:testableexamples // calls a remote API
	c, err := client.New(
		client.Config{ResponseHeaderTimeout: 60 * time.Second, MaxIdleConnsPerHost: 50},
		client.WithRoundTripper(bearer("secret")),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.example.com/v1/complete", nil)
	if err != nil {
		log.Panic(err)
	}

	resp, err := c.Do(req)
	if err != nil {
		log.Panic(err)
	}
	defer resp.Body.Close()
}

// Every request goes through the RoundTripper wrappers; the call is bounded by
// its context.
func ExampleWithRoundTripper() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, r.Header.Get("Authorization"))
	}))
	defer api.Close()

	c, err := client.New(client.Config{}, client.WithRoundTripper(bearer("secret")))
	if err != nil {
		log.Panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, api.URL, nil)
	if err != nil {
		log.Panic(err)
	}

	resp, err := c.Do(req)
	if err != nil {
		log.Panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Panic(err)
	}

	fmt.Print(string(body))
	// Output:
	// Bearer secret
}
