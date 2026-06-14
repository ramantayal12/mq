// Package object holds the object-storage backend for partition logs: an S3-compatible
// ObjectStore (MinIO locally, GCS's S3-interop endpoint in prod) and, in later phases, the
// ObjectLog that implements storage.Backend on top of it. This file is just the store
// abstraction and its minio-go implementation; nothing here is wired into the broker yet.
package object

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectStore is the minimal object-storage contract the object backend needs: write a
// whole object, read a byte range out of one, list keys under a prefix, and delete. One
// implementation (minio-go) serves both MinIO and GCS via config.
type ObjectStore interface {
	// Put writes data as the object at key, overwriting any existing object.
	Put(ctx context.Context, key string, data []byte) error
	// Get returns length bytes of the object at key starting at off. length <= 0 reads to
	// the end of the object.
	Get(ctx context.Context, key string, off, length int64) ([]byte, error)
	// List returns the keys of all objects whose name starts with prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// Delete removes the object at key. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
}

// Config points the store at a bucket. Endpoint may carry an http:// or https:// scheme
// (the scheme decides TLS); a bare host:port defaults to non-TLS.
type Config struct {
	Endpoint  string // "http://localhost:9000" (MinIO) or "https://storage.googleapis.com" (GCS)
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string // GCS interop wants one set; MinIO ignores it
}

// minioStore is the minio-go implementation of ObjectStore.
type minioStore struct {
	client *minio.Client
	bucket string
}

// NewMinIO builds an ObjectStore backed by minio-go.
func NewMinIO(cfg Config) (ObjectStore, error) {
	endpoint, secure := splitScheme(cfg.Endpoint)
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: secure,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("object: new minio client: %w", err)
	}
	return &minioStore{client: client, bucket: cfg.Bucket}, nil
}

// splitScheme strips a leading http:// or https:// from endpoint and reports whether TLS
// should be used. A scheme-less endpoint defaults to non-TLS (the MinIO dev case).
func splitScheme(endpoint string) (host string, secure bool) {
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		return strings.TrimPrefix(endpoint, "https://"), true
	case strings.HasPrefix(endpoint, "http://"):
		return strings.TrimPrefix(endpoint, "http://"), false
	default:
		return endpoint, false
	}
}

func (s *minioStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("object: put %q: %w", key, err)
	}
	return nil
}

func (s *minioStore) Get(ctx context.Context, key string, off, length int64) ([]byte, error) {
	opts := minio.GetObjectOptions{}
	if off > 0 || length > 0 {
		// SetRange is inclusive on both ends; length<=0 means "to end" (open-ended range).
		end := int64(0)
		if length > 0 {
			end = off + length - 1
		}
		if err := opts.SetRange(off, end); err != nil {
			return nil, fmt.Errorf("object: get %q set range: %w", key, err)
		}
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("object: get %q: %w", key, err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("object: read %q: %w", key, err)
	}
	return data, nil
}

func (s *minioStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("object: list %q: %w", prefix, obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (s *minioStore) Delete(ctx context.Context, key string) error {
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("object: delete %q: %w", key, err)
	}
	return nil
}
