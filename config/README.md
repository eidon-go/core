# config

Layered service configuration read through a small generic API. You describe your config structs
in your own code and fill them with getters — there is no reflection-based struct binding.

```go
cfg, err := config.New(ctx,
    config.WithParser(yaml.Parse, yaml.Extensions()...), // github.com/eidon-go/core/contrib/config/yaml
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

Nested objects are flattened to dotted keys: `{"http": {"port": 8080}}` is `http.port`. Two
entries of one document (or one `WithDefaults` map) that flatten to the same key — `{"http":
{"port": 1}, "http.port": 2}` — are an error (`ErrKeyCollision`): `New` fails, a watched update is
rejected and the previous values stay. The same key in different sources is ordinary layering. A
KV key may be listed only once across `WithKeys` and `WithWatchedKeys` (`ErrDuplicateKey`).

### Formats

The format of a file or KV key follows its extension (case-insensitive). JSON (`.json`) is built
in; other formats are plugged in with `WithParser(p, exts...)`, where `""` means "no extension".
YAML lives in a separate module so the core stays dependency-free:

```go
import "github.com/eidon-go/core/contrib/config/yaml"

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
config.Get[T](cfg, key) T                    // zero value when unset or mistyped (mistyped: logged once)
config.GetOrDefault(cfg, key, def) T         // def when unset or mistyped (mistyped: logged once)
config.Lookup[T](cfg, key) (T, error)        // *KeyError: ErrKeyMissing or ErrKeyMistyped
config.Live[T](cfg, key) func() T            // re-reads on every call; reconfigures nothing by itself
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

### Nested values, maps and lists of objects

```go
db := cfg.Sub("db")                          // a view: Get[string](db, "host") reads "db.host"
for _, name := range cfg.Keys("tenants") {   // direct children, sorted: ["a", "b"]
    limit := config.Get[int](cfg.Sub("tenants."+name), "limit")
}
routes, err := cfg.Items("routes")           // a list of objects: one *Config per element
for _, route := range routes {
    host := config.Get[string](route, "host")
}
```

- A `Sub` view reads through every layer (the environment included: `DB_HOST`), follows hot
  reload and can be passed to anything that takes a `*config.Config`. It is not a component:
  register the root `cfg` with `app.App`.
- `Keys` sees the files, KV documents and defaults, not the environment (a variable name cannot be
  mapped back to a key): a key set only by an environment variable is read by `Get` but not listed.
- `Items` copies each element: no environment layer, no defaults, no hot reload — read it again
  in `OnChange`. A value that is not a list of objects is a `*KeyError` wrapping `ErrKeyMistyped`.
- **Keys with dots.** A map key that contains a dot (`"api.example.com": {...}`) cannot be told
  from nesting after flattening: it reads as `api` → `example` → `com`. Keep such names in values,
  in a list of objects read with `Items` (`[{host: api.example.com, ...}]`).

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
Prefer `Optional` over `GetOrDefault` for typed settings: `GetOrDefault` falls back on a mistyped
value (it only logs it, once per key), `Optional` reports it.

## Remote configuration and hot reload

A backend implements the small `config.KV` interface (`Get`, `Watch`, `Close`); the etcd v3
backend lives in the separate module `github.com/eidon-go/core/contrib/config/etcd`.

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
- Every watch (the first one and each one after a reconnect) starts with the key's current
  state, then delivers its later states in order — that is the `KV.Watch` contract. So changes
  made between `New` and `Run` or while disconnected are not lost, and values never go back to an
  older state. If the first event is an error, not a state, or does not arrive within
  `WithKVTimeout`, the watch is reopened with backoff.
- A changed document replaces the key's previous values (keys removed from it disappear); a
  deleted key removes its values; an invalid document is logged and the previous values stay.
- A failed watch reconnects with backoff (`WithWatchBackoff(initial, max)`, default 1s doubling
  up to 30s, with jitter). The backoff resets only after a healthy watch: one that delivered a
  change after its initial state, or stayed open for at least the maximum delay after it.
- Every KV read, and the wait for a watch's initial state, is bounded by `WithKVTimeout`
  (default 5s). `New` reads every key once: if the backend cannot answer in time, `New` fails.
  Watch errors are logged through `WithLogger`.
- An environment variable overriding a key pins it: hot reload does not change it.
- `WithOptionalWatchedKeys` adds watched keys that may not exist yet: `New` treats a missing one
  as an empty document (as the watch treats a deleted key) instead of failing with
  `ErrKVKeyNotFound`, and the watch picks it up once it is created.

### Reacting to changes

`Live` is a getter: it reconfigures nothing built from an earlier value. To rebuild something —
a rate limiter, a pool — subscribe with `OnChange`:

```go
stop := cfg.OnChange("ratelimit", func() {   // "" for any key; relative to a Sub view
    limiter.SetLimit(rate.Limit(config.Get[float64](cfg, "ratelimit.rps")))
})
defer stop()
```

The callback runs after the new values are visible, when an update changed a key at or below the
prefix, on the config's notification goroutine: the callbacks of one change run one after another
in registration order. The watcher does not wait for them — a slow callback delays only the next
callbacks, never the new values — and the updates published while callbacks run are merged into
one notification. Start heavy or blocking work in a goroutine of its own, so that it does not hold
up the other callbacks. A panic in a callback is recovered and logged.

### Writing a KV backend

`Get` returns the value or an error wrapping `config.ErrKVKeyNotFound`. `Watch` must send the
key's current state first (`KVEventPut`, or `KVEventDelete` for a missing key), then the key's
later states in order. A backend that only sees the latest value (polling, blocking queries) may
skip intermediate states, but never sends a state older than one already sent, and always
follows a change with the latest state. A backend whose watch is registered asynchronously takes
the initial state and the start of the stream from the same point of the store's history (in
etcd, read the key and watch from the revision after that read). Report a failure as an event
with `Err` set and close the channel. `Close` is called once, after the context of every watch
was cancelled; it must tolerate backend goroutines that are still finishing.

## Tests

`config.FromMap(map[string]any{...})` returns a `Config` with just those values and no
environment layer. For `New`, combine `WithFS(fstest.MapFS{...})` and
`WithEnvLookup(mapLookup)` — both work with `t.Parallel()`.
