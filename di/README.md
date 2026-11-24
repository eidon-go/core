# di

Configuration loader for services built on core. Loads from yaml files,
env vars, and a remote KV backend such as etcd v3 (static + dynamic with
push-Watch). Generic typed API. Lock-free reads via atomic snapshot.

## Quick start

```go
cfg, err := di.NewConfig(ctx)
// with etcd (separate module github.com/eidon-go/core/di/etcd):
// cfg, err := di.NewConfig(ctx, di.WithKV(etcd.Factory))
if err != nil {
    log.Fatal(err)
}

port := di.Get[string](cfg, "http.public.port")
timeout := di.GetOrDefault[time.Duration](cfg, "http.timeout", 30*time.Second)
currentRPS := di.Live[int](cfg, "limits.rps") // re-read on each call() for runtime tunables
```

## Sources and priority

```
env vars  >  etcd dynamic  >  etcd static  >  yaml file  >  defaults
```

Any key can be overridden via env. If env is absent (or set to `""`), the
highest-priority source with a value wins: dynamic > static > file >
default. See [Unset values](#unset-values).

| Layer         | Who manages       | When changes                              |
|---------------|-------------------|-------------------------------------------|
| env           | operator (deploy) | process restart                           |
| etcd dynamic  | operator (live)   | runtime — pushed via etcd Watch           |
| etcd static   | operator (deploy) | process restart                           |
| yaml file     | repo / image      | image rebuild                             |
| defaults      | core / app code   | code change                               |

## Bootstrap configuration

The following keys are read from **env only** (they decide where other
configuration comes from):

- `APP_ENV` (default: `dev`) — environment label, exposed via
  `cfg.GetAppEnv()`.
- `APP_SERVICE_NAME` — required when etcd is configured. Exposed via
  `cfg.GetServiceName()`.
- `APP_CONFIG_FILE_PATHS` — comma-separated list of yaml file paths or
  directories (directories load `config.yaml`).
- `APP_CONFIG_FILE_PATH` — legacy single path.
- `APP_CONFIG_ETCD_ENDPOINT` — etcd address (required when any etcd
  path is set).
- `APP_CONFIG_ETCD_STATIC_PATHS` — comma-separated list of paths read
  once at startup.
- `APP_CONFIG_ETCD_DYNAMIC_PATHS` — comma-separated list of paths
  watched for live updates.
- `APP_CONFIG_ETCD_PATH` — legacy single dynamic path.
- `APP_CONFIG_ETCD_REQUEST_TIMEOUT` — Go-duration string ("5s",
  "500ms", "1m") bounding every etcd Get during static load and
  watcher resync. Default `5s`. A malformed value fails startup with
  `ErrInvalidEtcdRequestTimeout`.

Etcd paths are passed to etcd as-is: core does not prepend `APP_ENV` or
`APP_SERVICE_NAME`. Template the full key (including any namespace prefix
your etcd setup requires) into the env var at deploy time.

## Public API

```go
func Get[T ValueType](cfg *Config, key string) T
func GetOrDefault[T ValueType](cfg *Config, key string, def T) T
func Live[T ValueType](cfg *Config, key string) func() T
func LiveOrDefault[T ValueType](cfg *Config, key string, def T) func() T
```

- `Get` — read once.
- `GetOrDefault` — read once with a fallback.
- `Live` — closure that re-reads on every call. Use for hot-reloadable
  values (rate limits, feature flags) that may change at runtime via
  etcd dynamic.
- `LiveOrDefault` — `Live` with a fallback.

### Required keys

```go
func Require[T ValueType](cfg *Config, key string) (T, error)
func NewReader(cfg *Config) *Reader
// generic methods, Go 1.27+
func (r *Reader) Required[T ValueType](key string) T
func (r *Reader) Get[T ValueType](key string) T
func (r *Reader) GetOrDefault[T ValueType](key string, def T) T
func (r *Reader) Err() error
```

- `Require` — read once; returns `ErrRequiredKeyMissing` when the key is
  not set in any source, `ErrRequiredKeyType` when the value can't be
  coerced to `T`, and `ErrNilConfig` for a nil `*Config`. The error names
  the key.
- `Reader` — reads a whole config in one pass. `Required` collects those
  errors; `Get` / `GetOrDefault` read optional keys exactly like the
  package functions and never fail. `Err()` joins every `Required`
  failure (in read order for sequential reads), so a deploy reports all
  missing keys at once instead of one per restart. `Live` /
  `LiveOrDefault` stay on `*Config`: they are for runtime reads, a
  `Reader` is a startup tool.

```go
r := di.NewReader(cfg)

smtpCfg := smtp.Config{
    Host:     r.Required[string]("smtp.host"),
    Port:     r.Required[int]("smtp.port"),
    Username: r.Get[string]("smtp.username"),
    Timeout:  r.GetOrDefault[time.Duration]("smtp.timeout", 10*time.Second),
}
if err := r.Err(); err != nil {
    return Config{}, fmt.Errorf("load config: %w", err)
}
```

Required means **set**, not non-empty: `""` in yaml, `0` and an empty slice
all pass. Validate values (URLs, emails, ranges) where they are used.

### Unset values

Every reader (`Get`, `GetOrDefault`, `Live`, `Require`, `Reader`) skips
values that only look set, and falls through to the next source:

- an env var set to `""` — what `KEY=${KEY}` in docker-compose / k8s
  produces when `KEY` is unset on the host;
- a `null` value — `key:` with nothing after it in yaml or etcd.

So `SMTP_HOST=""` does not hide `smtp.host` from yaml, and a `null` does
not hide a lower source: a later yaml file, etcd static or an etcd dynamic
PUT with `key:` keeps the value from the earlier yaml files (or the
default). A key whose only values are these is
missing. To clear a value through env, set the key to something your code
treats as "off" instead of `""`.

The `Reader` methods are generic methods, so this package requires
Go 1.27+.

`ValueType` constraint:
`string | int | int64 | bool | float64 | time.Duration | []string`.

Methods on `*Config`: `GetAppEnv()`, `GetServiceName()` — captured from
env at startup, immutable for the process lifetime.

## Defaults semantics

`GetOrDefault` returns the default **only when the key is absent or the
stored value can't be coerced to T**. It does NOT return the default
when the value is zero (`0`, `""`, empty slice). If you want
zero-as-absent semantics, wrap manually:

```go
v := di.Get[int](cfg, "k")
if v <= 0 {
    v = fallback
}
```

## Remote config backends

The core module ships no remote backend, so it pulls in no etcd/grpc
dependencies. A backend implements the small `di.KV` interface
(`Get`, `Watch`, `Close`) and is plugged in with `di.WithKV`:

```go
import "github.com/eidon-go/core/di/etcd"

cfg, err := di.NewConfig(ctx, di.WithKV(etcd.Factory))
```

The `app.config.etcd.*` env vars keep their names whatever the backend.
If any etcd path is configured but no backend was passed, `NewConfig`
fails with `di.ErrKVBackendRequired` — configured paths are never
silently ignored. A backend must wrap `di.ErrKVKeyNotFound` for absent
keys.

## Lifecycle

`NewConfig` is **fail-fast**: errors when etcd is configured but no
backend was passed or the backend is unreachable, when paths can't be
parsed, or when `APP_SERVICE_NAME` is empty with etcd configured. After
successful start, watch errors are logged but do not propagate —
operators can fix etcd without restarting the service.

The dynamic watcher runs until `ctx` is cancelled, then closes its KV.
Pass a long-lived `ctx` (typically the app's main context).

## Container helper

`Container[C, S]` and `NewContainer` provide a two-step bootstrap (init
config → init services). `ServicesInit` may return a `Rollback` invoked
**only when initialization fails** — it releases partially-constructed
resources. Runtime teardown of healthy resources is the job of
`lifecycle.Resource` registered with `app.App`; on success the
`Rollback` is discarded. See `container.go` godoc for details.

For consumers that embed the config (`type AppConfig struct { *di.Config; ... }`),
use `di.Get[T](appCfg.Config, "k")` — explicit access to the embedded
pointer.
