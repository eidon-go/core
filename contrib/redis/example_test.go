package redis_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/eidon-go/core/contrib/redis"
)

func Example() {
	server, err := miniredis.Run() // a real server in production
	if err != nil {
		fmt.Println(err)

		return
	}
	defer server.Close()

	client, err := redis.New(redis.Config{Addrs: []string{server.Addr()}})
	if err != nil {
		fmt.Println(err)

		return
	}

	ctx := context.Background()

	// app.App calls Start and Shutdown when the client is registered with it.
	if err := client.Start(ctx); err != nil {
		fmt.Println(err)

		return
	}
	defer client.Shutdown(ctx) //nolint:errcheck // example

	rdb := client.Redis()

	if err := rdb.Set(ctx, "greeting", "hello", time.Minute).Err(); err != nil {
		fmt.Println(err)

		return
	}

	greeting, err := rdb.Get(ctx, "greeting").Result()
	if err != nil {
		fmt.Println(err)

		return
	}

	_, err = rdb.Get(ctx, "missing").Result()

	fmt.Println(greeting, errors.Is(err, goredis.Nil))
	// Output: hello true
}
