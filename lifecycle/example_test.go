package lifecycle_test

import (
	"context"
	"fmt"

	"github.com/eidon-go/core/lifecycle"
)

type client struct{ name string }

func (c *client) Shutdown(context.Context) error {
	fmt.Println("close", c.name)

	return nil
}

func ExampleGroup() {
	var cache *client // never constructed: skipped

	infra := lifecycle.Group(&client{name: "db"}, &client{name: "queue"}, cache)

	_ = infra.Shutdown(context.Background())

	// Output:
	// close queue
	// close db
}
