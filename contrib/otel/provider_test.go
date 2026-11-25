package otel_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/eidon-go/core/contrib/otel"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     otel.Config
		wantErr []string
	}{
		{name: "all off", cfg: otel.Config{ServiceName: "svc"}},
		{
			name: "otlp on",
			cfg:  otel.Config{ServiceName: "svc", Endpoint: "https://collector:4318/otlp", Traces: true},
		},
		{name: "endpoint without signals", cfg: otel.Config{ServiceName: "svc", Endpoint: "http://collector:4318"}},
		{name: "no service name", cfg: otel.Config{}, wantErr: []string{"serviceName is required"}},
		{
			name:    "signal without endpoint",
			cfg:     otel.Config{ServiceName: "svc", Metrics: true},
			wantErr: []string{"endpoint is required"},
		},
		{
			name:    "host:port is not a URL",
			cfg:     otel.Config{ServiceName: "svc", Endpoint: "collector:4318", Logs: true},
			wantErr: []string{"scheme must be http or https"},
		},
		{
			name:    "no host",
			cfg:     otel.Config{ServiceName: "svc", Endpoint: "http:///v1"},
			wantErr: []string{"host is missing"},
		},
		{
			name:    "query",
			cfg:     otel.Config{ServiceName: "svc", Endpoint: "http://collector:4318?a=b"},
			wantErr: []string{"query, fragment and user info are not allowed"},
		},
		{
			name:    "unparsable",
			cfg:     otel.Config{ServiceName: "svc", Endpoint: "http://[::1"},
			wantErr: []string{"endpoint"},
		},
		{name: "ipv6", cfg: otel.Config{ServiceName: "svc", Endpoint: "http://[::1]:4318", Traces: true}},
		{
			name:    "signal path",
			cfg:     otel.Config{ServiceName: "svc", Endpoint: "http://collector:4318/otlp/v1/traces/"},
			wantErr: []string{"drop the /v1/<signal> suffix"},
		},
		{
			name:    "escaped slash",
			cfg:     otel.Config{ServiceName: "svc", Endpoint: "http://collector:4318/a%2Fb"},
			wantErr: []string{"escaped slashes"},
		},
		{
			name:    "empty attribute key",
			cfg:     otel.Config{ServiceName: "svc", ResourceAttributes: map[string]string{"": "x"}},
			wantErr: []string{"resourceAttributes has an empty key"},
		},
		{
			name: "everything wrong at once",
			cfg: otel.Config{
				Traces:         true,
				ExportTimeout:  -time.Second,
				MetricInterval: -time.Second,
			},
			wantErr: []string{
				"serviceName is required", "endpoint is required",
				"exportTimeout must not be negative", "metricInterval must not be negative",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.wantErr) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, otel.ErrInvalidConfig)

			for _, want := range tt.wantErr {
				require.ErrorContains(t, err, want)
			}

			_, err = otel.New(tt.cfg)
			require.ErrorIs(t, err, otel.ErrInvalidConfig)
		})
	}
}

func TestNew_AllOff(t *testing.T) {
	t.Parallel()

	p, err := otel.New(otel.Config{ServiceName: "svc"})
	require.NoError(t, err)

	_, span := p.TracerProvider().Tracer("t").Start(t.Context(), "op")
	assert.False(t, span.SpanContext().IsValid(), "a no-op tracer makes invalid spans")
	span.End()

	assert.Equal(t, otel.DefaultName, p.Name())
	require.NoError(t, p.ForceFlush(t.Context()))
	require.NoError(t, p.Shutdown(t.Context()))
}

func TestNew_OTLPExportsEverySignal(t *testing.T) {
	t.Parallel()

	collector := newCollector(t)
	srv := httptest.NewServer(collector)
	t.Cleanup(srv.Close)

	p, err := otel.New(otel.Config{
		ServiceName:        "svc",
		ServiceVersion:     "1.2.3",
		Environment:        "test",
		ResourceAttributes: map[string]string{"team": "core", "service.name": "overridden"},
		Endpoint:           srv.URL + "/prefix/",
		Headers:            map[string]string{"Authorization": "Bearer token"},
		Traces:             true,
		Metrics:            true,
		Logs:               true,
	})
	require.NoError(t, err)

	emitAll(t, p)
	require.NoError(t, p.Shutdown(t.Context()))

	got := collector.snapshot()
	assert.ElementsMatch(t, []string{"/prefix/v1/traces", "/prefix/v1/metrics", "/prefix/v1/logs"}, got.paths)
	assert.Equal(t, []string{"Bearer token", "Bearer token", "Bearer token"}, got.auth)

	wantResource := map[string]string{
		"service.name":                "svc",
		"service.version":             "1.2.3",
		"deployment.environment.name": "test",
		"team":                        "core",
		"telemetry.sdk.language":      "go",
	}

	for _, signal := range []string{"traces", "metrics", "logs"} {
		require.Contains(t, got.resources, signal)

		for k, v := range wantResource {
			assert.Equal(t, v, got.resources[signal][k], "%s: resource attribute %s", signal, k)
		}
	}

	assert.Equal(t, []string{"op"}, got.spans)
	assert.Equal(t, []string{"requests"}, got.metrics)
	assert.Equal(t, []string{"hello"}, got.logs)
}

func TestNew_EndpointWithoutPath(t *testing.T) {
	t.Parallel()

	collector := newCollector(t)
	srv := httptest.NewServer(collector)
	t.Cleanup(srv.Close)

	headers := map[string]string{"Authorization": "Bearer token"}

	p, err := otel.New(otel.Config{ServiceName: "svc", Endpoint: srv.URL, Headers: headers, Traces: true})
	require.NoError(t, err)

	headers["Authorization"] = "changed after New"

	emitAll(t, p)
	require.NoError(t, p.ForceFlush(t.Context()))

	got := collector.snapshot()
	assert.Equal(t, []string{"/v1/traces"}, got.paths, "ForceFlush exports before Shutdown")
	assert.Equal(t, []string{"Bearer token"}, got.auth, "New copies Config.Headers")
	require.NoError(t, p.Shutdown(t.Context()))
}

func TestNew_HTTPSWithTLSConfig(t *testing.T) {
	t.Parallel()

	collector := newCollector(t)
	srv := httptest.NewTLSServer(collector)
	t.Cleanup(srv.Close)

	transport, ok := srv.Client().Transport.(*http.Transport)
	require.True(t, ok)

	p, err := otel.New(
		otel.Config{ServiceName: "svc", Endpoint: srv.URL, Traces: true},
		otel.WithTLSConfig(&tls.Config{RootCAs: transport.TLSClientConfig.RootCAs, MinVersion: tls.VersionTLS12}),
	)
	require.NoError(t, err)

	emitAll(t, p)
	require.NoError(t, p.Shutdown(t.Context()))
	assert.Equal(t, []string{"op"}, collector.snapshot().spans)
}

func TestNew_ProcessorsTurnSignalsOn(t *testing.T) {
	t.Parallel()

	spans := tracetest.NewInMemoryExporter()
	reader := sdkmetric.NewManualReader()
	logs := &recordingProcessor{}

	p, err := otel.New(otel.Config{ServiceName: "svc"},
		otel.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(spans)),
		otel.WithMetricReader(reader),
		otel.WithLogProcessor(logs),
		otel.WithSpanProcessor(nil),
		otel.WithMetricReader(nil),
		otel.WithLogProcessor(nil),
		nil,
	)
	require.NoError(t, err)

	emitAll(t, p)

	require.Len(t, spans.GetSpans(), 1)
	assert.Equal(t, "op", spans.GetSpans()[0].Name)
	assert.Equal(t, "svc", resourceValue(spans.GetSpans()[0].Resource.Attributes(), "service.name"))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	assert.Equal(t, "requests", rm.ScopeMetrics[0].Metrics[0].Name)

	assert.Equal(t, []string{"hello"}, logs.bodies())

	require.NoError(t, p.Shutdown(t.Context()))
	assert.True(t, logs.isShutdown(), "the Provider shuts down the processors it was given")
	assert.Empty(t, spans.GetSpans(), "the in-memory exporter is reset by its Shutdown")
}

func TestNew_ProviderOptions(t *testing.T) {
	t.Parallel()

	spans := tracetest.NewInMemoryExporter()

	p, err := otel.New(otel.Config{ServiceName: "svc"},
		otel.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(spans)),
		otel.WithTracerProviderOptions(sdktrace.WithSampler(sdktrace.NeverSample())),
		otel.WithMeterProviderOptions(),
		otel.WithLoggerProviderOptions(),
		otel.WithResourceOptions(),
	)
	require.NoError(t, err)

	emitAll(t, p)
	assert.Empty(t, spans.GetSpans(), "the sampler from WithTracerProviderOptions drops every span")
	require.NoError(t, p.Shutdown(t.Context()))
}

func TestProvider_ShutdownOnce(t *testing.T) {
	t.Parallel()

	logs := &recordingProcessor{shutdownErr: errors.New("boom")}

	p, err := otel.New(otel.Config{ServiceName: "svc"}, otel.WithLogProcessor(logs), otel.WithName("telemetry"))
	require.NoError(t, err)

	err = p.Shutdown(t.Context())
	require.ErrorContains(t, err, "otel: shutdown logs: boom")
	require.NoError(t, p.Shutdown(t.Context()), "a repeated Shutdown returns nil")
	assert.Equal(t, 1, logs.shutdownCalls())
}

func TestProvider_ConcurrentShutdownHonorsItsContext(t *testing.T) {
	t.Parallel()

	logs := &recordingProcessor{entered: make(chan struct{}), release: make(chan struct{})}

	p, err := otel.New(otel.Config{ServiceName: "svc"}, otel.WithLogProcessor(logs))
	require.NoError(t, err)

	first := make(chan error, 1)

	go func() { first <- p.Shutdown(context.Background()) }()

	<-logs.entered

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, p.Shutdown(ctx), context.Canceled, "a concurrent Shutdown stops waiting when its ctx is done")

	close(logs.release)
	require.NoError(t, <-first)
	require.NoError(t, p.Shutdown(t.Context()), "after the first Shutdown returned")
	assert.Equal(t, 1, logs.shutdownCalls())
}

func TestProvider_SetGlobal(t *testing.T) { //nolint:paralleltest // changes the otel globals
	// The global providers delegate to the first ones set and cannot be
	// restored, so only the propagators and the error handler are put back.
	prevMP, prevLP := otelapi.GetMeterProvider(), otelapi.GetLoggerProvider()
	prevProp, prevHandler := otelapi.GetTextMapPropagator(), otelapi.GetErrorHandler()

	t.Cleanup(func() {
		otelapi.SetTextMapPropagator(prevProp)
		otelapi.SetErrorHandler(prevHandler)
	})

	spans := tracetest.NewInMemoryExporter()

	var logBuf syncBuffer

	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	p, err := otel.New(otel.Config{ServiceName: "svc"},
		otel.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(spans)),
		otel.WithPropagators(propagation.TraceContext{}),
		otel.WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	p.SetGlobal()

	assert.Same(t, p.TracerProvider(), otelapi.GetTracerProvider())
	assert.Equal(t, prevMP, otelapi.GetMeterProvider(), "metrics are off: the global stays")
	assert.Equal(t, prevLP, otelapi.GetLoggerProvider(), "logs are off: the global stays")
	assert.Equal(t, propagation.TraceContext{}, p.Propagators())
	assert.Equal(t, propagation.TraceContext{}, otelapi.GetTextMapPropagator())

	_, span := otelapi.Tracer("global").Start(t.Context(), "op")
	span.End()
	require.Len(t, spans.GetSpans(), 1)

	otelapi.Handle(errors.New("export failed"))
	assert.Contains(t, logBuf.String(), "export failed")
	assert.Contains(t, logBuf.String(), "component=otel")

	require.NoError(t, p.Shutdown(t.Context()))
	otelapi.SetTextMapPropagator(propagation.Baggage{})
	p.SetGlobal()
	assert.Equal(t, propagation.Baggage{}, otelapi.GetTextMapPropagator(), "SetGlobal does nothing after Shutdown")
}

func TestProvider_Lifecycle(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(newCollector(t))
	t.Cleanup(srv.Close)

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		p, err := otel.New(otel.Config{
			ServiceName: "svc", Endpoint: srv.URL, Traces: true, Metrics: true, Logs: true,
		})
		require.NoError(t, err)
		emitAll(t, p)

		return p
	})
}

func TestNew_SchemeWinsOverEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")

	collector := newCollector(t)
	srv := httptest.NewTLSServer(collector)
	t.Cleanup(srv.Close)

	transport, ok := srv.Client().Transport.(*http.Transport)
	require.True(t, ok)

	p, err := otel.New(
		otel.Config{ServiceName: "svc", Endpoint: srv.URL, Traces: true, Metrics: true, Logs: true},
		otel.WithTLSConfig(&tls.Config{RootCAs: transport.TLSClientConfig.RootCAs, MinVersion: tls.VersionTLS12}),
	)
	require.NoError(t, err)

	emitAll(t, p)
	require.NoError(t, p.Shutdown(t.Context()))
	assert.ElementsMatch(t, []string{"/v1/traces", "/v1/metrics", "/v1/logs"}, collector.snapshot().paths,
		"https is used although the env asks for insecure")
}

func TestNew_CertificateEnvWithHTTPEndpoint(t *testing.T) {
	tlsSrv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsSrv.Close)

	certFile := filepath.Join(t.TempDir(), "ca.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tlsSrv.Certificate().Raw})
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", certFile)

	collector := newCollector(t)
	srv := httptest.NewServer(collector)
	t.Cleanup(srv.Close)

	p, err := otel.New(otel.Config{ServiceName: "svc", Endpoint: srv.URL, Traces: true, Metrics: true, Logs: true})
	require.NoError(t, err, "a certificate variable does not break a plain-HTTP endpoint")

	emitAll(t, p)
	require.NoError(t, p.Shutdown(t.Context()))
	assert.ElementsMatch(t, []string{"/v1/traces", "/v1/metrics", "/v1/logs"}, collector.snapshot().paths)
}

func TestNew_HeadersReplaceEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-env=1")

	tests := []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{name: "env headers without Config.Headers", want: "1"},
		{name: "Config.Headers replace them", headers: map[string]string{"Authorization": "Bearer t"}, want: ""},
	}

	for _, tt := range tests { //nolint:paralleltest // the parent uses t.Setenv, so it cannot be parallel
		t.Run(tt.name, func(t *testing.T) {
			collector := newCollector(t)
			srv := httptest.NewServer(collector)
			t.Cleanup(srv.Close)

			p, err := otel.New(otel.Config{ServiceName: "svc", Endpoint: srv.URL, Headers: tt.headers, Traces: true})
			require.NoError(t, err)

			emitAll(t, p)
			require.NoError(t, p.Shutdown(t.Context()))
			assert.Equal(t, []string{tt.want}, collector.snapshot().envHeader)
		})
	}
}

func TestNew_EnvResourceAttributes(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "team=env,service.name=from-env")

	spans := tracetest.NewInMemoryExporter()

	p, err := otel.New(otel.Config{ServiceName: "svc"}, otel.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(spans)))
	require.NoError(t, err)

	emitAll(t, p)
	require.Len(t, spans.GetSpans(), 1)

	attrs := spans.GetSpans()[0].Resource.Attributes()
	assert.Equal(t, "env", resourceValue(attrs, "team"), "the SDK merges the env attributes underneath")
	assert.Equal(t, "svc", resourceValue(attrs, "service.name"), "Config wins")
	require.NoError(t, p.Shutdown(t.Context()))
}

func TestNew_MalformedEnvResourceIsLogged(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "malformed")

	var logBuf syncBuffer

	p, err := otel.New(otel.Config{ServiceName: "svc"},
		otel.WithLogProcessor(&recordingProcessor{}),
		otel.WithResourceOptions(resource.WithFromEnv()),
		otel.WithLogger(slog.New(slog.NewTextHandler(&logBuf, nil))),
	)
	require.NoError(t, err, "a malformed variable must not keep the service from starting")
	assert.Contains(t, logBuf.String(), "resource detection incomplete")
	require.NoError(t, p.Shutdown(t.Context()))
}

func TestNew_ResourceSchemaConflictIsLogged(t *testing.T) {
	t.Parallel()

	var logBuf syncBuffer

	spans := tracetest.NewInMemoryExporter()

	p, err := otel.New(otel.Config{ServiceName: "svc"},
		otel.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(spans)),
		otel.WithResourceOptions(resource.WithDetectors(oldSchemaDetector{})),
		otel.WithLogger(slog.New(slog.NewTextHandler(&logBuf, nil))),
	)
	require.NoError(t, err, "a detector built against another semconv version must not fail New")
	assert.Contains(t, logBuf.String(), "conflicting Schema URL")

	emitAll(t, p)
	require.Len(t, spans.GetSpans(), 1)
	assert.Equal(t, "svc", resourceValue(spans.GetSpans()[0].Resource.Attributes(), "service.name"))
	require.NoError(t, p.Shutdown(t.Context()))
}

func TestNew_FailureReleasesOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  otel.Config
		opts []otel.Option
	}{
		{name: "invalid config", cfg: otel.Config{}},
		{
			name: "tls with an http endpoint",
			cfg:  otel.Config{ServiceName: "svc", Endpoint: "http://collector:4318", Traces: true},
			opts: []otel.Option{otel.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logs := &recordingProcessor{}
			reader := sdkmetric.NewManualReader()

			opts := append([]otel.Option{otel.WithLogProcessor(logs), otel.WithMetricReader(reader)}, tt.opts...)

			_, err := otel.New(tt.cfg, opts...)
			require.ErrorIs(t, err, otel.ErrInvalidConfig)

			assert.Equal(t, 1, logs.shutdownCalls(), "New shuts down the processors it was given")
			assert.ErrorIs(t, reader.Collect(t.Context(), &metricdata.ResourceMetrics{}), sdkmetric.ErrReaderShutdown)
		})
	}
}

// oldSchemaDetector stands for a detector built against an older semconv.
type oldSchemaDetector struct{}

func (oldSchemaDetector) Detect(context.Context) (*resource.Resource, error) {
	return resource.NewWithAttributes(
		"https://opentelemetry.io/schemas/1.26.0",
		attribute.String("detected", "yes"),
	), nil
}

func emitAll(t *testing.T, p *otel.Provider) {
	t.Helper()

	_, span := p.TracerProvider().Tracer("test").Start(t.Context(), "op")
	span.End()

	counter, err := p.MeterProvider().Meter("test").Int64Counter("requests")
	require.NoError(t, err)
	counter.Add(t.Context(), 1)

	var rec log.Record
	rec.SetBody(attribute.StringValue("hello"))
	p.LoggerProvider().Logger("test").Emit(t.Context(), rec)
}

// collector is a fake OTLP/HTTP receiver that decodes what it gets.
type collector struct {
	collected

	t  *testing.T
	mu sync.Mutex
}

type collected struct {
	paths     []string
	auth      []string
	envHeader []string
	resources map[string]map[string]string // signal → resource attributes
	spans     []string
	metrics   []string
	logs      []string
}

func newCollector(t *testing.T) *collector {
	t.Helper()

	return &collector{t: t, collected: collected{resources: map[string]map[string]string{}}}
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.paths = append(c.paths, r.URL.Path)
	c.auth = append(c.auth, r.Header.Get("Authorization"))
	c.envHeader = append(c.envHeader, r.Header.Get("X-Env"))

	switch {
	case strings.HasSuffix(r.URL.Path, "/v1/traces"):
		c.recordTraces(body)
	case strings.HasSuffix(r.URL.Path, "/v1/metrics"):
		c.recordMetrics(body)
	case strings.HasSuffix(r.URL.Path, "/v1/logs"):
		c.recordLogs(body)
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

func (c *collector) recordTraces(body []byte) {
	var req collectortrace.ExportTraceServiceRequest
	c.decode(body, &req)

	for _, rs := range req.GetResourceSpans() {
		c.resources["traces"] = attrs(rs.GetResource())

		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				c.spans = append(c.spans, s.GetName())
			}
		}
	}
}

func (c *collector) recordMetrics(body []byte) {
	var req collectormetrics.ExportMetricsServiceRequest
	c.decode(body, &req)

	for _, rm := range req.GetResourceMetrics() {
		c.resources["metrics"] = attrs(rm.GetResource())

		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				c.metrics = append(c.metrics, m.GetName())
			}
		}
	}
}

func (c *collector) recordLogs(body []byte) {
	var req collectorlogs.ExportLogsServiceRequest
	c.decode(body, &req)

	for _, rl := range req.GetResourceLogs() {
		c.resources["logs"] = attrs(rl.GetResource())

		for _, sl := range rl.GetScopeLogs() {
			for _, l := range sl.GetLogRecords() {
				c.logs = append(c.logs, l.GetBody().GetStringValue())
			}
		}
	}
}

func (c *collector) decode(body []byte, m proto.Message) {
	if err := proto.Unmarshal(body, m); err != nil {
		c.t.Errorf("decode %T: %v", m, err)
	}
}

func (c *collector) snapshot() collected {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.collected
}

func attrs(r *resourcepb.Resource) map[string]string {
	out := map[string]string{}
	for _, kv := range r.GetAttributes() {
		out[kv.GetKey()] = anyString(kv.GetValue())
	}

	return out
}

func anyString(v *commonpb.AnyValue) string { return v.GetStringValue() }

func resourceValue(kvs []attribute.KeyValue, key string) string {
	for _, kv := range kvs {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}

	return ""
}

// recordingProcessor is an in-memory sdklog.Processor.
type recordingProcessor struct {
	shutdownErr error
	entered     chan struct{} // closed when Shutdown is entered, if set
	release     chan struct{} // Shutdown waits for it, if set

	mu       sync.Mutex
	records  []string
	shutdown int
}

func (p *recordingProcessor) OnEmit(_ context.Context, r *sdklog.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.records = append(p.records, r.Body().AsString())

	return nil
}

func (p *recordingProcessor) Shutdown(context.Context) error {
	p.mu.Lock()
	p.shutdown++
	p.mu.Unlock()

	if p.entered != nil {
		close(p.entered)
		<-p.release
	}

	return p.shutdownErr
}

func (*recordingProcessor) ForceFlush(context.Context) error { return nil }

func (*recordingProcessor) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }

func (p *recordingProcessor) bodies() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.records...)
}

func (p *recordingProcessor) isShutdown() bool { return p.shutdownCalls() > 0 }

func (p *recordingProcessor) shutdownCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.shutdown
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}
