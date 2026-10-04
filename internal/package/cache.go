package packagemanager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	contractpackage "github.com/well-prado/new-blok/contract/package"
)

const (
	maxCacheEntries   = contractpackage.DefaultStoreMaxPackages
	maxCacheFileBytes = contractpackage.MaxBundleBytes + 1
	maxCacheBytes     = contractpackage.DefaultStoreMaxArtifactBytes + (maxCacheEntries * 64 << 10)
	maxLockFileBytes  = 16 << 20
)

type cacheIndex struct {
	Identity contractpackage.Identity `json:"identity"`
	Digest   string                   `json:"digest"`
}

// Cache stores immutable content-addressed bundles behind an exact identity
// index. A package version can never silently switch to different content.
type Cache struct{ root string }

func NewCache(root string) (*Cache, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("package: cache directory is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("package: resolve cache directory: %w", err)
	}
	for _, dir := range []string{abs, filepath.Join(abs, "blobs"), filepath.Join(abs, "indexes")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("package: create cache directory")
		}
	}
	return &Cache{root: abs}, nil
}

func (c *Cache) Put(ctx context.Context, bundle contractpackage.Bundle, policy contractpackage.TrustPolicy, env contractpackage.Environment) error {
	if c == nil {
		return fmt.Errorf("package: cache is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	verified, err := bundle.Verify(policy, env)
	if err != nil {
		return err
	}
	data, err := json.Marshal(bundle)
	if err != nil {
		return fmt.Errorf("package: encode verified bundle")
	}
	if len(data) > maxCacheFileBytes {
		return fmt.Errorf("package: encoded bundle exceeds cache limit")
	}
	unlock, err := lockCache(ctx, c.root)
	if err != nil {
		return err
	}
	defer unlock()
	indexPath := c.indexPath(verified.Identity)
	if existing, _, err := c.readVerifiedLocked(ctx, verified.Identity, policy, env); err == nil {
		old, _ := existing.Manifest.Digest()
		if old != verified.ManifestDigest || existing.Manifest.ArtifactDigest != verified.ArtifactDigest || !signaturesEqual(existing.Signature, bundle.Signature) {
			return contractpackage.ErrVersionConflict
		}
		return nil
	} else if !errors.Is(err, contractpackage.ErrOfflineMissing) {
		return err
	}
	indexes, err := os.ReadDir(filepath.Join(c.root, "indexes"))
	if err != nil {
		return fmt.Errorf("package: read cache indexes")
	}
	if len(indexes) >= maxCacheEntries {
		return fmt.Errorf("package: offline cache capacity exceeded")
	}
	blobDigest := fileDigest(data)
	blobPath := filepath.Join(c.root, "blobs", digestHex(blobDigest)+".bundle")
	newBlob := false
	if existing, err := readBounded(blobPath, maxCacheFileBytes); err == nil {
		if fileDigest(existing) != blobDigest || !bytesEqual(existing, data) {
			return fmt.Errorf("package: content-addressed cache blob is corrupt")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		newBlob = true
	} else {
		return fmt.Errorf("package: read cache blob")
	}
	if err := checkBlobCapacity(c.root, blobPath, int64(len(data))); err != nil {
		return err
	}
	if newBlob {
		if err := atomicWrite(blobPath, data, 0o600); err != nil {
			return fmt.Errorf("package: commit cache blob: %w", err)
		}
	}
	encodedIndex, err := json.Marshal(cacheIndex{Identity: verified.Identity, Digest: blobDigest})
	if err != nil {
		return fmt.Errorf("package: encode cache index")
	}
	if err := atomicWrite(indexPath, encodedIndex, 0o600); err != nil {
		return fmt.Errorf("package: commit cache index: %w", err)
	}
	return nil
}

func checkBlobCapacity(root, newBlob string, newSize int64) error {
	entries, err := os.ReadDir(filepath.Join(root, "blobs"))
	if err != nil {
		return fmt.Errorf("package: read cache blobs")
	}
	if len(entries) > maxCacheEntries {
		return fmt.Errorf("package: offline cache blob count exceeds limit")
	}
	var total int64
	newAlreadyPresent := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".bundle") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("package: inspect cache blob")
		}
		if info.Size() > maxCacheFileBytes {
			return fmt.Errorf("package: cache blob exceeds size limit")
		}
		if int64(info.Size()) > maxCacheBytes-total {
			return fmt.Errorf("package: offline cache byte capacity exceeded")
		}
		total += info.Size()
		if filepath.Join(root, "blobs", entry.Name()) == newBlob {
			newAlreadyPresent = true
		}
	}
	if !newAlreadyPresent && newSize > maxCacheBytes-total {
		return fmt.Errorf("package: offline cache byte capacity exceeded")
	}
	return nil
}

func (c *Cache) Get(ctx context.Context, id contractpackage.Identity, policy contractpackage.TrustPolicy, env contractpackage.Environment) (contractpackage.Bundle, contractpackage.Verified, error) {
	if c == nil {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: cache is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return contractpackage.Bundle{}, contractpackage.Verified{}, err
	}
	unlock, err := lockCache(ctx, c.root)
	if err != nil {
		return contractpackage.Bundle{}, contractpackage.Verified{}, err
	}
	defer unlock()
	return c.readVerifiedLocked(ctx, id, policy, env)
}

func (c *Cache) readVerifiedLocked(ctx context.Context, id contractpackage.Identity, policy contractpackage.TrustPolicy, env contractpackage.Environment) (contractpackage.Bundle, contractpackage.Verified, error) {
	if err := ctx.Err(); err != nil {
		return contractpackage.Bundle{}, contractpackage.Verified{}, err
	}
	indexData, err := readBounded(c.indexPath(id), 4096)
	if errors.Is(err, os.ErrNotExist) {
		return contractpackage.Bundle{}, contractpackage.Verified{}, &contractpackage.Error{Code: "offline_package_missing", Path: id.Name, Message: "exact package identity is not present in the verified offline cache"}
	}
	if err != nil {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: corrupted cache identity index")
	}
	var index cacheIndex
	decoder := json.NewDecoder(strings.NewReader(string(indexData)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&index); err != nil || index.Identity != id || !isFileDigest(index.Digest) || !isJSONEOF(decoder) {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: corrupted cache identity index")
	}
	canonicalIndex, err := json.Marshal(index)
	if err != nil || !bytesEqual(canonicalIndex, indexData) {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: cache identity index is not canonical JSON")
	}
	blobPath := filepath.Join(c.root, "blobs", digestHex(index.Digest)+".bundle")
	data, err := readBounded(blobPath, maxCacheFileBytes)
	if err != nil {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: cached artifact blob is missing or unreadable")
	}
	if fileDigest(data) != index.Digest {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: cached artifact blob digest mismatch")
	}
	var bundle contractpackage.Bundle
	decoder = json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: corrupted cached package bundle")
	}
	canonical, err := json.Marshal(bundle)
	if err != nil || !bytesEqual(canonical, data) {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: cached package bundle is not canonical JSON")
	}
	verified, err := bundle.Verify(policy, env)
	if err != nil {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: cached bundle verification failed: %w", err)
	}
	if verified.Identity != id {
		return contractpackage.Bundle{}, contractpackage.Verified{}, fmt.Errorf("package: cache index identity does not match bundle")
	}
	return bundle, verified, nil
}

func (c *Cache) indexPath(id contractpackage.Identity) string {
	key := id.Name + "@" + id.Version
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(c.root, "indexes", hex.EncodeToString(sum[:])+".json")
}

// CacheSource uses exact verified cache identities offline. Online resolution
// asks the upstream source for candidates and stores each accepted exact bundle.
type CacheSource struct {
	Cache   *Cache
	Remote  contractpackage.Source
	Policy  contractpackage.TrustPolicy
	Env     contractpackage.Environment
	Offline bool
}

func (s CacheSource) Versions(ctx context.Context, name string) ([]contractpackage.Identity, error) {
	if s.Cache == nil {
		return nil, fmt.Errorf("package: verified cache is required")
	}
	if !s.Offline && s.Remote != nil {
		return s.Remote.Versions(ctx, name)
	}
	unlock, err := lockCache(ctx, s.Cache.root)
	if err != nil {
		return nil, err
	}
	defer unlock()
	entries, err := os.ReadDir(filepath.Join(s.Cache.root, "indexes"))
	if err != nil {
		return nil, fmt.Errorf("package: read cache indexes")
	}
	if len(entries) > maxCacheEntries {
		return nil, fmt.Errorf("package: cache index count exceeds limit")
	}
	ids := make([]contractpackage.Identity, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := readBounded(filepath.Join(s.Cache.root, "indexes", entry.Name()), 4096)
		if err != nil {
			return nil, fmt.Errorf("package: corrupted cache identity index")
		}
		var index cacheIndex
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&index); err != nil || index.Identity.Validate() != nil || !isFileDigest(index.Digest) || !isJSONEOF(decoder) {
			return nil, fmt.Errorf("package: corrupted cache identity index")
		}
		canonicalIndex, err := json.Marshal(index)
		if err != nil || !bytesEqual(canonicalIndex, data) || entry.Name() != filepath.Base(s.Cache.indexPath(index.Identity)) {
			return nil, fmt.Errorf("package: cache identity index is not canonical or is stored under the wrong identity")
		}
		if index.Identity.Name != name {
			continue
		}
		if _, _, err := s.Cache.readVerifiedLocked(ctx, index.Identity, s.Policy, s.Env); err != nil {
			return nil, err
		}
		ids = append(ids, index.Identity)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Version < ids[j].Version })
	return ids, nil
}

func (s CacheSource) Get(ctx context.Context, id contractpackage.Identity) (contractpackage.Bundle, error) {
	if s.Cache == nil {
		return contractpackage.Bundle{}, fmt.Errorf("package: verified cache is required")
	}
	if s.Offline || s.Remote == nil {
		bundle, _, err := s.Cache.Get(ctx, id, s.Policy, s.Env)
		return bundle, err
	}
	bundle, err := s.Remote.Get(ctx, id)
	if err != nil {
		return contractpackage.Bundle{}, err
	}
	if _, err := bundle.Verify(s.Policy, s.Env); err != nil {
		return contractpackage.Bundle{}, err
	}
	if err := s.Cache.Put(ctx, bundle, s.Policy, s.Env); err != nil {
		return contractpackage.Bundle{}, err
	}
	return bundle, nil
}

// FetchLocked retrieves precisely the identities recorded by an existing lock,
// then verifies all signed manifest and artifact digests before returning.
func (s CacheSource) FetchLocked(ctx context.Context, lock contractpackage.Lock) ([]contractpackage.Bundle, error) {
	if s.Cache == nil {
		return nil, fmt.Errorf("package: verified cache is required")
	}
	return contractpackage.VerifyLock(ctx, lock, s, s.Policy, s.Env)
}

// WriteLock atomically stores canonical lock bytes and returns their SHA-256.
func WriteLock(path string, lock contractpackage.Lock) (string, error) {
	data, err := lock.Canonical()
	if err != nil {
		return "", err
	}
	if len(data) > maxLockFileBytes {
		return "", fmt.Errorf("package: lock exceeds 16 MiB")
	}
	if err := atomicWrite(path, data, 0o600); err != nil {
		return "", fmt.Errorf("package: commit lock atomically: %w", err)
	}
	return fileDigest(data), nil
}

func ReadLock(path string) (contractpackage.Lock, string, error) {
	data, err := readBounded(path, maxLockFileBytes)
	if err != nil {
		return contractpackage.Lock{}, "", fmt.Errorf("package: read lock")
	}
	if err := preflightLock(data); err != nil {
		return contractpackage.Lock{}, "", err
	}
	var lock contractpackage.Lock
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lock); err != nil {
		return contractpackage.Lock{}, "", fmt.Errorf("package: malformed package lock")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return contractpackage.Lock{}, "", fmt.Errorf("package: trailing lock data")
	}
	if err := lock.Validate(); err != nil {
		return contractpackage.Lock{}, "", err
	}
	canonical, err := lock.Canonical()
	if err != nil {
		return contractpackage.Lock{}, "", err
	}
	if !bytesEqual(canonical, data) {
		return contractpackage.Lock{}, "", fmt.Errorf("package: lock is not canonical JSON")
	}
	return lock, fileDigest(canonical), nil
}

// preflightLock counts attacker-controlled arrays before unmarshalling them
// into the full lock graph. The file-size bound limits each individual token.
func preflightLock(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return fmt.Errorf("package: malformed package lock")
	}
	var roots, packages, native int
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("package: malformed package lock")
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("package: malformed package lock")
		}
		switch key {
		case "roots":
			count, err := preflightArray(decoder, contractpackage.MaxResolvedPackages-roots, nil)
			if err != nil {
				return err
			}
			roots += count
		case "packages":
			count, err := preflightArray(decoder, contractpackage.MaxResolvedPackages-packages, func(item []byte) error {
				var pkg struct {
					Dependencies []json.RawMessage `json:"dependencies"`
				}
				if err := json.Unmarshal(item, &pkg); err != nil || len(pkg.Dependencies) > contractpackage.MaxDependencies {
					return fmt.Errorf("package: lock dependency count exceeds its limit")
				}
				return nil
			})
			if err != nil {
				return err
			}
			packages += count
		case "native":
			count, err := preflightArray(decoder, 16-native, func(item []byte) error {
				var lock struct {
					Packages []json.RawMessage `json:"packages"`
				}
				if err := json.Unmarshal(item, &lock); err != nil || len(lock.Packages) > 16384 {
					return fmt.Errorf("package: native lock package count exceeds its limit")
				}
				return nil
			})
			if err != nil {
				return err
			}
			native += count
		default:
			var ignored json.RawMessage
			if err := decoder.Decode(&ignored); err != nil {
				return fmt.Errorf("package: malformed package lock")
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("package: malformed package lock")
	}
	if !isJSONEOF(decoder) {
		return fmt.Errorf("package: trailing lock data")
	}
	return nil
}

func preflightArray(decoder *json.Decoder, remaining int, check func([]byte) error) (int, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return 0, fmt.Errorf("package: malformed lock collection")
	}
	count := 0
	for decoder.More() {
		if count >= remaining {
			return 0, fmt.Errorf("package: lock collection count exceeds its documented limit")
		}
		var item json.RawMessage
		if err := decoder.Decode(&item); err != nil {
			return 0, fmt.Errorf("package: malformed lock collection")
		}
		if check != nil {
			if err := check(item); err != nil {
				return 0, err
			}
		}
		count++
	}
	if _, err := decoder.Token(); err != nil {
		return 0, fmt.Errorf("package: malformed lock collection")
	}
	return count, nil
}

func isJSONEOF(decoder *json.Decoder) bool {
	var trailing json.RawMessage
	return errors.Is(decoder.Decode(&trailing), io.EOF)
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(absolute)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".blok-package-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, absolute); err != nil {
		return err
	}
	parent, err := os.Open(dir)
	if err == nil {
		defer parent.Close()
		if err := parent.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func readBounded(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, fmt.Errorf("package: file exceeds configured limit")
	}
	return data, nil
}

func fileDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func digestHex(digest string) string { return strings.TrimPrefix(digest, "sha256:") }
func isFileDigest(digest string) bool {
	if len(digest) != 71 || !strings.HasPrefix(digest, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(digest[7:])
	return err == nil
}
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func signaturesEqual(a, b *contractpackage.Signature) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Algorithm == b.Algorithm && a.KeyID == b.KeyID && a.Value == b.Value
}
