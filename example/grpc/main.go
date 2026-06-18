package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"kv_store_demo/internal/client"
)

func mustPut(ctx context.Context, c *client.Client, key, value string) {
	if err := c.Put(ctx, []byte(key), []byte(value)); err != nil {
		log.Fatalf("put %q: %v", key, err)
	}
}

func mustDelete(ctx context.Context, c *client.Client, key string) {
	if err := c.Delete(ctx, []byte(key)); err != nil {
		log.Fatalf("delete %q: %v", key, err)
	}
}

func mustFlush(ctx context.Context, c *client.Client) {
	if err := c.Flush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}
}

func mustCompact(ctx context.Context, c *client.Client) {
	if err := c.Compact(ctx); err != nil {
		log.Fatalf("compact: %v", err)
	}
}

func assertValue(ctx context.Context, c *client.Client, key, want string) {
	got, err := c.Get(ctx, []byte(key))
	if err != nil {
		log.Fatalf("get %q: %v", key, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		log.Fatalf("get %q = %q, want %q", key, got, want)
	}
}

func assertEmpty(ctx context.Context, c *client.Client, key string) {
	got, err := c.Get(ctx, []byte(key))
	if err != nil {
		log.Fatalf("get %q: %v", key, err)
	}
	if len(got) != 0 {
		log.Fatalf("get %q = %q, want empty value", key, got)
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "gRPC server address")
	timeout := flag.Duration("timeout", 5*time.Second, "example timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	c, err := client.Dial(ctx, *addr)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer c.Close()

	mustPut(ctx, c, "name", "alice")
	fmt.Println("stage 1: put name=alice")

	assertValue(ctx, c, "name", "alice")
	fmt.Println("stage 2: verified name=alice")

	mustPut(ctx, c, "name", "bob")
	assertValue(ctx, c, "name", "bob")
	fmt.Println("stage 3: updated and verified name=bob")

	mustDelete(ctx, c, "name")
	assertEmpty(ctx, c, "name")
	fmt.Println("stage 4: deleted and verified name is empty")

	mustPut(ctx, c, "city", "hangzhou")
	mustFlush(ctx, c)
	mustCompact(ctx, c)
	assertValue(ctx, c, "city", "hangzhou")
	fmt.Println("stage 5: flushed, compacted, and verified city=hangzhou")

	fmt.Println("verification succeeded")
}
