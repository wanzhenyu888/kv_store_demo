package client

import (
	"context"
	"errors"

	kvv1 "kv_store_demo/gen/kv/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client kvv1.KVServiceClient
}

func newClient(conn *grpc.ClientConn) *Client {
	return &Client{
		conn:   conn,
		client: kvv1.NewKVServiceClient(conn),
	}
}

func Dial(ctx context.Context, target string) (*Client, error) {
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	if err := waitReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return newClient(conn), nil
}

func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return errors.New("grpc client connection is shut down")
		}

		conn.Connect()
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	_, err := c.client.Put(ctx, &kvv1.PutRequest{
		Key:   key,
		Value: value,
	})
	return mapError(err)
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	resp, err := c.client.Get(ctx, &kvv1.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, mapError(err)
	}
	return resp.GetValue(), nil
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, err := c.client.Delete(ctx, &kvv1.DeleteRequest{
		Key: key,
	})
	return mapError(err)
}

func (c *Client) Flush(ctx context.Context) error {
	_, err := c.client.Flush(ctx, &kvv1.FlushRequest{})
	return mapError(err)
}

func (c *Client) Compact(ctx context.Context) error {
	_, err := c.client.Compact(ctx, &kvv1.CompactRequest{})
	return mapError(err)
}
