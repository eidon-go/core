# config/yaml

YAML support for [`github.com/eidon-go/core/config`](../README.md), in a separate module so the
core module stays free of third-party dependencies.

```sh
go get github.com/eidon-go/core/config/yaml
```

```go
import (
    "github.com/eidon-go/core/config"
    "github.com/eidon-go/core/config/yaml"
)

cfg, err := config.New(ctx,
    config.WithParser(yaml.Parse, yaml.Extensions()...), // .yaml, .yml, no extension
    config.WithFiles("config.yaml"),
)
```

- `Parse` matches `config.Parser`; it does not import `config`, so it works with any `config`
  version.
- Mapping keys that are not strings (`200: ok`) become their string form (`codes.200`).
- An empty document is an empty config; only the first document of a multi-document stream is
  read; a top-level list is an error.
