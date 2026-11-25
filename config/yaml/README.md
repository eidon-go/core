# config/yaml

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/config/yaml.svg)](https://pkg.go.dev/github.com/eidon-go/core/config/yaml)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=config%2Fyaml%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=config%2Fyaml%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=config-yaml)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=config-yaml)

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
