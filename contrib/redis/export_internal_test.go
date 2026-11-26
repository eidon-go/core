package redis

import "time"

// SetPingTimeout shortens the bound of the lifecycle PING without a read
// timeout, for tests.
func SetPingTimeout(c *Client, d time.Duration) {
	if c.pingTimeout > 0 {
		c.pingTimeout = d
	}
}
