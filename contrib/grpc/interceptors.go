package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// unaryChain combines interceptors into one, the first outermost, and lets
// the RPCs of the built-in health service (bypassHealth) skip it.
func unaryChain(interceptors []grpc.UnaryServerInterceptor, bypassHealth bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if bypassHealth && isHealthMethod(info.FullMethod) {
			return handler(ctx, req)
		}

		return callUnary(interceptors, ctx, req, info, handler)
	}
}

func callUnary(
	interceptors []grpc.UnaryServerInterceptor,
	ctx context.Context, //nolint:revive // ctx follows the interceptor list it belongs to
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if len(interceptors) == 0 {
		return handler(ctx, req)
	}

	return interceptors[0](ctx, req, info, func(ctx context.Context, req any) (any, error) {
		return callUnary(interceptors[1:], ctx, req, info, handler)
	})
}

// streamChain combines interceptors into one, the first outermost, and lets
// the RPCs of the built-in health service (bypassHealth) skip it.
func streamChain(interceptors []grpc.StreamServerInterceptor, bypassHealth bool) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if bypassHealth && isHealthMethod(info.FullMethod) {
			return handler(srv, ss)
		}

		return callStream(interceptors, srv, ss, info, handler)
	}
}

func callStream(
	interceptors []grpc.StreamServerInterceptor,
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if len(interceptors) == 0 {
		return handler(srv, ss)
	}

	return interceptors[0](srv, ss, info, func(srv any, ss grpc.ServerStream) error {
		return callStream(interceptors[1:], srv, ss, info, handler)
	})
}
