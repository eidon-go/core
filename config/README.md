# config

Layered service configuration read through a small generic API. You describe your config structs
in your own code and fill them with getters — there is no reflection-based struct binding.

```go
cfg, err := config.New(ctx,
    config.WithParser(yaml.Parse, yaml.Extensions()...), // github.com/eidon-go/core/config/yaml
    config.WithFiles("config.yaml"),
    config.WithOptionalFiles("config.local.yaml"),
    config.WithDefaults(map[string]any{"http.timeout": "30s"}),
    config.WithEnvPrefix("APP"),
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
| `WithDefaults(map)` | lowest-priority values; nested maps or dotted keys; repeated calls merge |
| `WithFiles(paths...)` | documents merged in order; regular files up to 10 MiB |
| `WithOptionalFiles(paths...)` | like `WithFiles`, but a missing file is skipped |
| `WithFS(fs.FS)` | read files from an `embed.FS`, `fstest.MapFS`, … instead of the OS |
| `WithKV(kv)` + `WithKeys(keys...)` | documents read once from a KV backend |
| `WithKV(kv)` + `WithWatchedKeys(keys...)` | documents read and then watched for changes |
| environment | always on, see below |

Nested objects are flattened to dotted keys: `{"http": {"port": 8080}}` is `http.port`. A key may
be listed only once across `WithKeys` and `WithWatchedKeys` (`ErrDuplicateKey`).

### Formats

The format of a file or KV key follows its extension (case-insensitive). JSON (`.json`) is built
in; other formats are plugged in with `WithParser(p, exts...)`, where `""` means "no extension".
YAML lives in a separate module so the core stays dependency-free:

```go
import "github.com/eidon-go/core/config/yaml"

config.WithParser(yaml.Parse, yaml.Extensions()...) // .yaml, .yml and keys without extension
```

A `Parser` is just `func([]byte) (map[string]any, error)`, so any format can be added the same
way. A missing parser fails `New` with `ErrUnknownFormat`.

### Environment

Every key can be overridden by an environment variable: `http.public.port` → `HTTP_PUBLIC_PORT`
(dots and dashes become underscores). A variable that is empty or only whitespace, and a null
value in a document, count as **not set**, so a lower-priority source is used.

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

`T` is one of `string`, `int`, `int64`, `uint`, `uint64`, `bool`, `float64`, `time.Duration`,
`[]string`, `[]int` (the set may grow). Conversion is strict, so a typo surfaces as a type error
rather than a wrong value:

- integers: integers in range, whole floats, decimal strings — not `1.9`, not booleans; JSON
  integers keep full precision;
- `bool`: booleans and `true/false`, `yes/no`, `on/off`, `1/0` — not numbers;
- `time.Duration`: strings such as `"30s"` and the number `0` — a bare `30` has no unit and is
  rejected;
- `float64`: finite numbers only;
- lists: lists and comma-separated strings, element by element.

### Building config structs

A `Reader` collects every problem so a struct can be read in one literal and checked once:

```go
r := config.NewReader(cfg)
smtpCfg := smtp.Config{
    Host:    config.Required[string](r, "smtp.host"),          // missing or mistyped: error
    Port:    config.Required[int](r, "smtp.port"),
    Timeout: config.Optional(r, "smtp.timeout", 10*time.Second), // missing: default; mistyped: error
}
if err := r.Err(); err != nil {
    return err
}
```

`Err` joins one `*config.KeyError{Key, Err}` per problem; match with
`errors.Is(err, config.ErrKeyMissing)` / `config.ErrKeyMistyped` or `errors.As(err, &keyErr)`.
Prefer `Optional` over `GetOrDefault` for typed settings: `GetOrDefault` silently falls back on a
mistyped value.

## Remote configuration and hot reload

A backend implements the small `config.KV` interface (`Get`, `Watch`, `Close`); the etcd v3
backend lives in the separate module `github.com/eidon-go/core/config/etcd`.

```go
cfg, err := config.New(ctx,
    config.WithKV(kv),
    config.WithKeys("/svc/base.json"),
    config.WithWatchedKeys("/svc/flags.json"),
)
a.Add(cfg) // the watcher runs in cfg.Run
```

- `Config` implements `lifecycle.Runner`: `Run` watches the keys until shutdown, `Shutdown`
  stops it and closes the KV. Register it with `app.App`, or call `Run`/`Shutdown` yourself.
- Config takes ownership of the KV: `New` closes it on failure, `Shutdown` otherwise.
- Every time a watch is opened (the first time and after a reconnect), the key is re-read, so
  changes made between `New` and `Run` or while disconnected are not lost. If that read fails,
  the watch is reopened with backoff until it succeeds.
- A changed document replaces the key's previous values (keys removed from it disappear); a
  deleted key removes its values; an invalid document is logged and the previous values stay.
- A failed watch reconnects with backoff (`WithWatchBackoff(initial, max)`, default 1s doubling
  up to 30s, with jitter).
- Every KV read is bounded by `WithKVTimeout` (default 5s). Watch errors are logged through
  `WithLogger`.
- An environment variable overriding a key pins it: hot reload does not change it.

## Tests

`config.FromMap(map[string]any{...})` returns a `Config` with just those values and no
environment layer. For `New`, combine `WithFS(fstest.MapFS{...})` and
`WithEnvLookup(mapLookup)` — both work with `t.Parallel()`.
