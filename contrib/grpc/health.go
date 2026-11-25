package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/eidon-go/core/lifecycle"
)

// healthServiceName is the full name of the standard health service.
const healthServiceName = "grpc.health.v1.Health"

// healthService answers grpc.health.v1 from the server's state and a
// lifecycle.Healthchecker instead of statuses set by hand: SERVING while the
// server serves and readiness reports nil.
type healthService struct {
	healthpb.UnimplementedHealthServer

	server    *Server
	readiness lifecycle.Healthchecker
}

func registerHealth(s *Server, readiness lifecycle.Healthchecker) {
	healthpb.RegisterHealthServer(s.srv, &healthService{
		UnimplementedHealthServer: healthpb.UnimplementedHealthServer{},
		server:                    s,
		readiness:                 readiness,
	})
}

// Check answers for the empty name (the whole server) and every registered
// service, all with the same status; other names get NOT_FOUND, as the health
// protocol requires.
func (h *healthService) Check(
	ctx context.Context,
	req *healthpb.HealthCheckRequest,
) (*healthpb.HealthCheckResponse, error) {
	if svc := req.GetService(); svc != "" {
		if _, ok := h.server.srv.GetServiceInfo()[svc]; !ok {
			return nil, status.Errorf(codes.NotFound, "unknown service %q", svc)
		}
	}

	return &healthpb.HealthCheckResponse{Status: h.status(ctx)}, nil
}

// List reports the status of the whole server (the empty name) and of every
// registered service.
func (h *healthService) List(ctx context.Context, _ *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	servingStatus := h.status(ctx)
	services := h.server.srv.GetServiceInfo()
	statuses := make(map[string]*healthpb.HealthCheckResponse, len(services)+1)

	statuses[""] = &healthpb.HealthCheckResponse{Status: servingStatus}
	for name := range services {
		statuses[name] = &healthpb.HealthCheckResponse{Status: servingStatus}
	}

	return &healthpb.HealthListResponse{Statuses: statuses}, nil
}

func (h *healthService) status(ctx context.Context) healthpb.HealthCheckResponse_ServingStatus {
	if h.server.Healthcheck(ctx) != nil {
		return healthpb.HealthCheckResponse_NOT_SERVING
	}

	if h.readiness != nil && h.readiness.Healthcheck(ctx) != nil {
		return healthpb.HealthCheckResponse_NOT_SERVING
	}

	return healthpb.HealthCheckResponse_SERVING
}
