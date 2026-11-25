# config

Layered service configuration with a small generic read API.

```go
cfg, err := config.New(ctx,
    config.WithFiles("config.yaml"),
    config.WithDefaults(map[string]any{"http.timeout": "30s"}),
)
if err != nil {
    return err
}

port := config.Get[int](cfg, "http.port")
timeout := config.GetOrDefault(cfg, "http.timeout", 10*time.Second)
rps := config.Live[int](cfg, "limits.rps") // re-reads on every call: rps()
```

## Sources and priority

```
environment  >  watched KV keys  >  KV keys  >  files  >  defaults
```

| Option | Source |
|---|---|
| `WithDefaults(map)` | lowest-priority values; nested maps or dotted keys |
| `WithFiles(paths...)` | yaml (`.yaml`, `.yml`, no extension) or `.json`, merged in order; regular files up to 10 MiB |
| `WithFS(fs.FS)` | read files from an `embed.FS`, `fstest.MapFS`, … instead of the OS |
| `WithKV(kv)` + `WithKeys(keys...)` | yaml/json documents read once from a KV backend |
| `WithKV(kv)` + `WithWatchedKeys(keys...)` | documents read and then watched for changes |
| environment | always on, see below |

Nested maps are flattened to dotted keys: `http: {port: 8080}` is `http.port`.

### Environment

Every key can be overridden by an environment variable: `http.public.port` → `HTTP_PUBLIC_PORT`
(dots and dashes become underscores). An environment variable set to `""` and a null value
(`key:` in yaml) count as **not set**, so a lower-priority source is used.

- `WithEnvPrefix("APP")` → `APP_HTTP_PUBLIC_PORT`. Recommended: it keeps keys such as `path`,
  `user` or `hostname` from being overridden by unrelated variables.
- `WithEnvLookup(fn)` replaces `os.LookupEnv` (tests); `WithEnvLookup(nil)` disables the layer.
- `WithEnvBootstrap()` lets the deployment choose sources via `CONFIG_FILES`,
  `CONFIG_KV_KEYS`, `CONFIG_KV_WATCHED_KEYS` (comma-separated, appended to the options) and
  `CONFIG_KV_TIMEOUT` (prefixed when `WithEnvPrefix` is set).

## Reading values

```go
config.Get[T](cfg, key) T                    // zero value when unset or mistyped
config.GetOrDefault(cfg, key, def) T         // def when unset or mistyped
config.Live[T](cfg, key) func() T            // re-reads on every call
config.LiveOrDefault(cfg, key, def) func() T
```

`T` is one of `string`, `int`, `int64`, `bool`, `float64`, `time.Duration`, `[]string`.
Conversion is strict, so a typo surfaces as a type error rather than a wrong value:

- `int`: integers, whole floats (json numbers), decimal strings — not `1.9`, not booleans;
- `bool`: booleans and `true/false`, `yes/no`, `on/off`, `1/0` — not numbers;
- `time.Duration`: strings such as `"30s"` and the number `0` — a bare `30` has no unit and is
  rejected;
- `[]string`: lists of scalars and comma-separated strings.

### Required keys

A `Reader` collects every missing or mistyped key so a struct can be read in one literal and
checked once:

```go
r := config.NewReader(cfg)
smtpCfg := smtp.Config{
    Host:    config.Required[string](r, "smtp.host"),
    Port:    config.Required[int](r, "smtp.port"),
    Timeout: config.GetOrDefault(cfg, "smtp.timeout", 10*time.Second),
}
if err := r.Err(); err != nil {
    return err
}
```

`Err` joins one `*config.KeyError{Key, Err}` per problem; match with
`errors.Is(err, config.ErrKeyMissing)` / `config.ErrKeyMistyped` or `errors.As(err, &keyErr)`.

## Remote configuration and hot reload

A backend implements the small `config.KV` interface (`Get`, `Watch`, `Close`); the etcd v3
backend lives in the separate module `github.com/eidon-go/core/config/etcd`.

```go
cfg, err := config.New(ctx,
    config.WithKV(kv),
    config.WithKeys("/svc/base.yaml"),
    config.WithWatchedKeys("/svc/flags.yaml"),
)
a.Add(cfg) // the watcher runs in cfg.Run
```

- `Config` implements `lifecycle.Runner`: `Run` watches the keys until shutdown, `Shutdown`
  stops it and closes the KV. Register it with `app.App`, or call `Run`/`Shutdown` yourself.
- Config takes ownership of the KV: `New` closes it on failure, `Shutdown` otherwise.
- A changed document replaces the key's previous values (keys removed from it disappear); a
  deleted key removes its values; an invalid document is logged and the previous values stay.
- A failed watch reconnects with backoff (`WithWatchBackoff`, default 1s ×2 up to 30s with
  jitter) and re-reads the key to catch up on missed changes.
- Every KV read is bounded by `WithKVTimeout` (default 5s). Watch errors are logged through
  `WithLogger`.
- An environment variable overriding a key pins it: hot reload does not change it.

## Tests

`config.FromMap(map[string]any{...})` returns a `Config` with just those values and no
environment layer. For `New`, combine `WithFS(fstest.MapFS{...})` and
`WithEnvLookup(mapLookup)` — both work with `t.Parallel()`.
