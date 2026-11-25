package lifecycletest_test

import (
	"context"
	"sync"
	"testing"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

// server follows the contract: Run returns on Shutdown or ctx cancel, and
// immediately when Shutdown came first.
type server struct {
	once sync.Once
	stop chan struct{}
}

func newServer() *server { return &server{once: sync.Once{}, stop: make(chan struct{})} }

func (s *server) Start(context.Context) error { return nil }

func (s *server) Run(ctx context.Context) error {
	select {
	case <-s.stop:
	case <-ctx.Done():
	}

	return nil
}

func (s *server) Shutdown(context.Context) error {
	s.once.Do(func() { close(s.stop) })

	return nil
}

func TestRunner(t *testing.T) {
	t.Parallel()

	lifecycletest.Runner(t, func(*testing.T) lifecycle.Runner { return newServer() })
}

func TestResource(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(*testing.T) lifecycle.Resource {
		return lifecycle.ShutdownFunc(func(context.Context) error { return nil })
	})
}
