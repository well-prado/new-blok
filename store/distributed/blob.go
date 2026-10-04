package distributed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var ErrBlobUnavailable = errors.New("distributed store: blob unavailable or failed verification")

// BlobRef is an immutable, content-addressed reference stored in the journal.
type BlobRef struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
	Size   int    `json:"size"`
}

// BlobStore is an explicitly selected object-store port. Constructing an
// adapter does not contact the service; every operation uses its caller's
// context and the caller owns service lifecycle.
type BlobStore interface {
	Put(context.Context, []byte) (BlobRef, error)
	Get(context.Context, BlobRef) ([]byte, error)
}

type S3BlobStore struct {
	client *minio.Client
	bucket string
}

// NewS3BlobStore configures one S3-compatible bucket without creating it or
// making a network request. Credentials and endpoint are supplied explicitly.
func NewS3BlobStore(endpoint, bucket, accessKey, secretKey string, secure bool) (*S3BlobStore, error) {
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		return nil, errors.New("distributed store: explicit S3 endpoint, bucket, and credentials are required")
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
		Region: "us-east-1",
	})
	if err != nil {
		return nil, fmt.Errorf("distributed store: configure S3 client: %w", err)
	}
	return &S3BlobStore{client: client, bucket: bucket}, nil
}

// EnsureBucket is an explicit provisioning operation for local spike tools.
func (s *S3BlobStore) EnsureBucket(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("distributed store: inspect S3 bucket: %w", err)
	}
	if exists {
		return nil
	}
	if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		return fmt.Errorf("distributed store: create S3 bucket: %w", err)
	}
	return nil
}

// RemoveBucket deletes only this adapter's explicitly selected bucket. It is
// intended for isolated synthetic benchmark cleanup and uses the caller's
// bounded context.
func (s *S3BlobStore) RemoveBucket(ctx context.Context) error {
	for object := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil {
			return fmt.Errorf("distributed store: list S3 bucket for cleanup: %w", object.Err)
		}
		if err := s.client.RemoveObject(ctx, s.bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("distributed store: remove S3 object during cleanup: %w", err)
		}
	}
	if err := s.client.RemoveBucket(ctx, s.bucket); err != nil {
		return fmt.Errorf("distributed store: remove S3 bucket: %w", err)
	}
	return nil
}

func (s *S3BlobStore) Put(ctx context.Context, payload []byte) (BlobRef, error) {
	if len(payload) == 0 || len(payload) > MaxPayloadBytes {
		return BlobRef{}, errors.New("distributed store: blob must be between 1 byte and the payload bound")
	}
	digest := sha256.Sum256(payload)
	encoded := hex.EncodeToString(digest[:])
	_, err := s.client.PutObject(ctx, s.bucket, encoded, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return BlobRef{}, fmt.Errorf("%w: upload: %v", ErrBlobUnavailable, err)
	}
	ref := BlobRef{Bucket: s.bucket, Key: encoded, SHA256: encoded, Size: len(payload)}
	if _, err := s.Get(ctx, ref); err != nil {
		return BlobRef{}, fmt.Errorf("%w: verify uploaded object: %v", ErrBlobUnavailable, err)
	}
	return ref, nil
}

func (s *S3BlobStore) Get(ctx context.Context, ref BlobRef) ([]byte, error) {
	if ref.Bucket != s.bucket || ref.Size <= 0 || ref.Size > MaxPayloadBytes || ref.Key != ref.SHA256 || len(ref.SHA256) != sha256.Size*2 {
		return nil, fmt.Errorf("%w: malformed or foreign reference", ErrBlobUnavailable)
	}
	if _, err := hex.DecodeString(ref.SHA256); err != nil {
		return nil, fmt.Errorf("%w: malformed digest", ErrBlobUnavailable)
	}
	object, err := s.client.GetObject(ctx, ref.Bucket, ref.Key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: fetch: %v", ErrBlobUnavailable, err)
	}
	defer object.Close()
	payload, err := io.ReadAll(io.LimitReader(object, MaxPayloadBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read: %v", ErrBlobUnavailable, err)
	}
	if len(payload) != ref.Size || len(payload) > MaxPayloadBytes {
		return nil, fmt.Errorf("%w: size mismatch", ErrBlobUnavailable)
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != ref.SHA256 {
		return nil, fmt.Errorf("%w: digest mismatch", ErrBlobUnavailable)
	}
	return payload, nil
}

// CommitBlob stages and verifies the immutable object before committing its
// reference under the same owner fence used for all journal events. A failed
// metadata commit may leave an orphan object; retention must wait through the
// upload/commit retry and backup inventory horizon before collecting it.
func (s *Store) CommitBlob(ctx context.Context, owner Owner, id string, blobs BlobStore, payload []byte) error {
	if blobs == nil {
		return errors.New("distributed store: explicit blob store is required")
	}
	ref, err := blobs.Put(ctx, payload)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(ref)
	if err != nil {
		return fmt.Errorf("distributed store: encode blob reference: %w", err)
	}
	return s.Commit(ctx, owner, id, "artifact-reference", encoded)
}

// ReadBlob resolves a committed reference only after the object bytes pass
// size and SHA-256 verification. A missing object never becomes output.
func (s *Store) ReadBlob(ctx context.Context, partition, id string, blobs BlobStore) ([]byte, error) {
	if blobs == nil {
		return nil, errors.New("distributed store: explicit blob store is required")
	}
	encoded, err := s.Read(ctx, partition, id)
	if err != nil || encoded == nil {
		return nil, err
	}
	var record event
	if err := json.Unmarshal(encoded, &record); err != nil {
		return nil, fmt.Errorf("distributed store: decode blob event: %w", err)
	}
	if record.Partition != partition || record.ID != id || record.Kind != "artifact-reference" {
		return nil, errors.New("distributed store: committed event is not a blob reference for this partition")
	}
	var ref BlobRef
	if err := json.Unmarshal(record.Payload, &ref); err != nil {
		return nil, fmt.Errorf("distributed store: decode blob reference: %w", err)
	}
	return blobs.Get(ctx, ref)
}
