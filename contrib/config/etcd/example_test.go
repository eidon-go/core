package etcd_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/config"
	"github.com/eidon-go/core/contrib/config/etcd"
)

// Configuration from etcd with hot reload: base.json is read once, flags.json
// is watched while the application runs.
func Example() { //nolint:testableexamples // needs an etcd cluster and serves until a signal arrives
	ctx := context.Background()

	kv, err := etcd.New(etcd.Config{
		Endpoints:   []string{"etcd-0:2379", "etcd-1:2379", "etcd-2:2379"},
		DialTimeout: 3 * time.Second,
	}, etcd.WithLogger(slog.Default()))
	if err != nil {
		panic(err)
	}

	cfg, err := config.New(ctx,
		config.WithKV(kv), // cfg owns kv from here on and closes it
		config.WithKeys("/svc/base.json"),
		config.WithWatchedKeys("/svc/flags.json"),
		config.WithKVTimeout(5*time.Second),
	)
	if err != nil {
		panic(err)
	}

	rateLimit := config.Live[int](cfg, "rate.limit") // follows /svc/flags.json
	_ = rateLimit

	a := app.New()
	a.Add(cfg) // the watcher runs in cfg.Run; Shutdown closes the KV

	if err := a.Run(ctx); err != nil {
		panic(err)
	}
}

// Validate reports every problem of a Config at once.
func ExampleConfig_Validate() {
	err := etcd.Config{Username: "app"}.Validate()
	fmt.Println(err)
	// Output:
	// etcd: invalid config: Endpoints is empty
	// etcd: invalid config: Username and Password must be set together
}
