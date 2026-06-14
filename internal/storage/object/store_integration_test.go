//go:build integration

// Object-store integration test. Drives a real MinIO via the S3 API, so it needs the
// docker-compose minio service up (and its bucket created):
//
//	docker compose up -d minio createbuckets
//	go test -tags=integration ./internal/storage/object/ -run Store
//
// Connection settings come from MQ_OBJECT_* env vars, defaulting to the compose service.
package object

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func testConfig() Config {
	get := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	return Config{
		Endpoint:  get("MQ_OBJECT_ENDPOINT", "http://localhost:9000"),
		Bucket:    get("MQ_OBJECT_BUCKET", "mq-data"),
		AccessKey: get("MQ_OBJECT_ACCESS_KEY", "minioadmin"),
		SecretKey: get("MQ_OBJECT_SECRET_KEY", "minioadmin"),
		Region:    get("MQ_OBJECT_REGION", "us-east-1"),
	}
}

func TestStorePutGetListDelete(t *testing.T) {
	ctx := context.Background()
	store, err := NewMinIO(testConfig())
	if err != nil {
		t.Fatalf("NewMinIO: %v", err)
	}

	const prefix = "test/store/"
	key := prefix + "obj-1"
	payload := []byte("the quick brown fox jumps over the lazy dog")

	// Put.
	if err := store.Put(ctx, key, payload); err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() { _ = store.Delete(context.Background(), key) })

	// Get (whole object).
	got, err := store.Get(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("Get whole: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Get whole = %q, want %q", got, payload)
	}

	// Get (ranged): "brown" is at offset 10, length 5.
	got, err = store.Get(ctx, key, 10, 5)
	if err != nil {
		t.Fatalf("Get range: %v", err)
	}
	if want := []byte("brown"); !bytes.Equal(got, want) {
		t.Fatalf("Get range = %q, want %q", got, want)
	}

	// List sees the key under the prefix.
	keys, err := store.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("List = %v, want [%q]", keys, key)
	}

	// Delete, then List is empty.
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	keys, err = store.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("List after delete = %v, want empty", keys)
	}
}
