package config_test

import (
	"context"
	"errors"
	"fmt"
	"testing/fstest"
	"time"

	"github.com/eidon-go/core/config"
)

func ExampleNew() {
	files := fstest.MapFS{
		"config.json": {Data: []byte(`{"http": {"port": 8080, "timeout": "15s"}}`)},
	}

	cfg, err := config.New(context.Background(),
		// For YAML add: config.WithParser(yaml.Parse, yaml.Extensions()...)
		// with github.com/eidon-go/core/config/yaml.
		config.WithFS(files), // omit to read from the OS file system
		config.WithFiles("config.json"),
		config.WithDefaults(map[string]any{"http.host": "0.0.0.0"}),
		config.WithEnvPrefix("APP"), // APP_HTTP_PORT overrides http.port
		config.WithEnvLookup(func(name string) (string, bool) {
			return map[string]string{"APP_HTTP_PORT": "9090"}[name], name == "APP_HTTP_PORT"
		}),
	)
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(config.Get[string](cfg, "http.host"))
	fmt.Println(config.Get[int](cfg, "http.port"))
	fmt.Println(config.Get[time.Duration](cfg, "http.timeout"))
	fmt.Println(config.GetOrDefault(cfg, "http.workers", 4))

	// Output:
	// 0.0.0.0
	// 9090
	// 15s
	// 4
}

func ExampleReader() {
	cfg := config.FromMap(map[string]any{
		"smtp": map[string]any{"host": "mail.local", "port": "five"},
	})

	r := config.NewReader(cfg)
	host := config.Required[string](r, "smtp.host")
	port := config.Required[int](r, "smtp.port")
	user := config.Required[string](r, "smtp.user")

	fmt.Printf("%q %d %q\n", host, port, user)

	err := r.Err()
	fmt.Println(errors.Is(err, config.ErrKeyMissing), errors.Is(err, config.ErrKeyMistyped))
	fmt.Println(err)

	// Output:
	// "mail.local" 0 ""
	// true true
	// config: value has the wrong type: want int, got string: smtp.port
	// config: required key is missing: smtp.user
}
