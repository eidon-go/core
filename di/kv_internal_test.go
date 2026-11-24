package di

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfig_KVBackendRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "static paths",
			env: map[string]string{
				"APP_SERVICE_NAME":             "svc",
				"APP_CONFIG_ETCD_ENDPOINT":     "127.0.0.1:0",
				"APP_CONFIG_ETCD_STATIC_PATHS": "s.yaml",
			},
		},
		{
			name: "dynamic paths",
			env: map[string]string{
				"APP_SERVICE_NAME":              "svc",
				"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
				"APP_CONFIG_ETCD_DYNAMIC_PATHS": "d.yaml",
			},
		},
		{
			name: "legacy single path",
			env: map[string]string{
				"APP_SERVICE_NAME":         "svc",
				"APP_CONFIG_ETCD_ENDPOINT": "127.0.0.1:0",
				"APP_CONFIG_ETCD_PATH":     "d.yaml",
			},
		},
		{
			name: "no endpoint still reports the missing backend first",
			env: map[string]string{
				"APP_CONFIG_ETCD_DYNAMIC_PATHS": "d.yaml",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newConfigWithDeps(t.Context(), configDeps{
				envLookup: envFn(tt.env),
				openFile:  noFileFn(),
				kvFactory: nil,
			})
			require.ErrorIs(t, err, ErrKVBackendRequired)
		})
	}
}

func TestNewConfig_NoEtcdPathsNeedsNoBackend(t *testing.T) {
	t.Parallel()

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{"APP_ENV": "prod"}),
		openFile:  noFileFn(),
		kvFactory: nil,
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", cfg.GetAppEnv())
}

func TestWithKV_SetsFactory(t *testing.T) {
	t.Parallel()

	fake := newFakeEtcdKV()
	fake.SetData("s.yaml", []byte("a: 1\n"))

	var (
		gotEndpoint string
		gotTimeout  time.Duration
	)

	deps := configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":                "svc",
			"APP_CONFIG_ETCD_ENDPOINT":        "etcd:2379",
			"APP_CONFIG_ETCD_STATIC_PATHS":    "s.yaml",
			"APP_CONFIG_ETCD_REQUEST_TIMEOUT": "750ms",
		}),
		openFile:  noFileFn(),
		kvFactory: nil,
	}

	WithKV(func(endpoint string, requestTimeout time.Duration) (KV, error) {
		gotEndpoint, gotTimeout = endpoint, requestTimeout

		return fake, nil
	})(&deps)

	cfg, err := newConfigWithDeps(t.Context(), deps)
	require.NoError(t, err)

	assert.Equal(t, 1, Get[int](cfg, "a"))
	assert.Equal(t, "etcd:2379", gotEndpoint)
	assert.Equal(t, 750*time.Millisecond, gotTimeout)
	assert.Equal(t, 1, fake.CloseCount(), "static-load KV must be closed after the load")
}
