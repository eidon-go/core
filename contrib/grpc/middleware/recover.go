// Package middleware provides gRPC server interceptors. Pass them to the
// contrib/grpc server with its WithUnaryInterceptors and
// WithStreamInterceptors options, or to a plain grpc-go server with
// grpc.ChainUnaryInterceptor and grpc.ChainStreamInterceptor.
package middleware

import (
	"context"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoverUnary returns an interceptor that recovers a panic in the handler
// and in the interceptors that run inside it, logs it at Error level with the
// method and the stack trace, and answers codes.Internal with a generic
// message — the panic value is not sent to the client. A nil logger means
// slog.Default().
//
// Put it inside logging (so the failed RPC is logged with its Internal code)
// and usually last; a panic in an interceptor outside it is not recovered and
// crashes the process, as in plain grpc-go.
func RecoverUnary(logger *slog.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}

	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) { //nolint:nonamedreturns // the deferred recover sets err
		defer func() {
			if rec := recover(); rec != nil {
				resp, err = nil, recovered(ctx, logger, info.FullMethod, rec)
			}
		}()

		return handler(ctx, req)
	}
}

// RecoverStream is RecoverUnary for streaming RPCs. Messages the handler has
// sent before the panic stay sent; the stream then ends with codes.Internal.
func RecoverStream(logger *slog.Logger) grpc.StreamServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}

	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = recovered(ss.Context(), logger, info.FullMethod, rec)
			}
		}()

		return handler(srv, ss)
	}
}

func recovered(ctx context.Context, logger *slog.Logger, method string, rec any) error {
	logger.ErrorContext(ctx, "grpc handler panic",
		slog.String("method", method),
		slog.Any("panic", rec),
		slog.String("stack", string(debug.Stack())),
	)

	return status.Error(codes.Internal, "internal error") //nolint:wrapcheck // grpc-go needs the status error itself
}
