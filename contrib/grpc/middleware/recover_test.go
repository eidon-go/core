package middleware_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/eidon-go/core/contrib/grpc/middleware"
)

func TestRecoverUnary(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer

	intercept := middleware.RecoverUnary(slog.New(slog.NewJSONHandler(&logs, nil)))
	info := &grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/Boom"}

	resp, err := intercept(t.Context(), "req", info, func(context.Context, any) (any, error) {
		panic("secret detail")
	})
	assert.Nil(t, resp)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.NotContains(t, err.Error(), "secret detail", "the panic value is not sent to the client")

	out := logs.String()
	assert.Contains(t, out, `"msg":"grpc handler panic"`)
	assert.Contains(t, out, `"method":"/pkg.Svc/Boom"`)
	assert.Contains(t, out, `"panic":"secret detail"`)
	assert.Contains(t, out, `"stack":`)
}

func TestRecoverUnary_PassesThrough(t *testing.T) {
	t.Parallel()

	intercept := middleware.RecoverUnary(nil)
	want := status.Error(codes.NotFound, "missing")

	resp, err := intercept(t.Context(), "req", &grpc.UnaryServerInfo{}, func(_ context.Context, req any) (any, error) {
		return req, want
	})
	assert.Equal(t, "req", resp)
	require.ErrorIs(t, err, want)
}

type fakeStream struct {
	grpc.ServerStream

	ctx context.Context //nolint:containedctx // a fake stream carries its context
}

func (s fakeStream) Context() context.Context { return s.ctx }

func TestRecoverStream(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer

	intercept := middleware.RecoverStream(slog.New(slog.NewJSONHandler(&logs, nil)))
	info := &grpc.StreamServerInfo{FullMethod: "/pkg.Svc/Watch"}

	err := intercept(nil, fakeStream{ctx: t.Context()}, info, func(any, grpc.ServerStream) error {
		panic("boom")
	})
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, logs.String(), `"method":"/pkg.Svc/Watch"`)

	want := status.Error(codes.Canceled, "client gone")
	err = middleware.RecoverStream(nil)(nil, fakeStream{ctx: t.Context()}, info, func(any, grpc.ServerStream) error {
		return want
	})
	require.ErrorIs(t, err, want)
}
