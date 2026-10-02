package runtime

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"sync"

	contract "github.com/well-prado/new-blok/contract/runtime"
)

var (
	ErrUnauthenticated = errors.New("worker transport: unauthenticated")
	ErrUnauthorized    = errors.New("worker transport: unauthorized")
	ErrBlobNotFound    = errors.New("worker transport: blob not found")
)

type Principal struct {
	Name         string
	Capabilities []contract.Capability
}
type Credential struct {
	Token        string
	Principal    string
	Capabilities []contract.Capability
}
type TokenAuthenticator struct{ tokens []storedToken }
type storedToken struct {
	digest    [32]byte
	principal Principal
}

func NewTokenAuthenticator(values []Credential) (*TokenAuthenticator, error) {
	out := &TokenAuthenticator{}
	for _, value := range values {
		if value.Token == "" || value.Principal == "" {
			return nil, ErrUnauthenticated
		}
		digest := sha256.Sum256([]byte(value.Token))
		out.tokens = append(out.tokens, storedToken{digest: digest, principal: Principal{Name: value.Principal, Capabilities: contract.SortedCapabilities(value.Capabilities)}})
	}
	if len(out.tokens) == 0 {
		return nil, ErrUnauthenticated
	}
	return out, nil
}
func (a *TokenAuthenticator) Authenticate(token string, requested []contract.Capability) (Principal, error) {
	if a == nil || token == "" {
		return Principal{}, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(token))
	for _, candidate := range a.tokens {
		if subtle.ConstantTimeCompare(digest[:], candidate.digest[:]) == 1 {
			if !containsCapabilities(candidate.principal.Capabilities, requested) {
				return Principal{}, ErrUnauthorized
			}
			return candidate.principal, nil
		}
	}
	return Principal{}, ErrUnauthenticated
}
func containsCapabilities(have, want []contract.Capability) bool {
	for _, value := range want {
		found := false
		for _, candidate := range have {
			if value == candidate {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

type BlobStore struct {
	mu     sync.Mutex
	max    int
	values map[string][]byte
	used   int
}

func NewBlobStore(max int) (*BlobStore, error) {
	if max <= 0 {
		return nil, contract.ErrLimitExceeded
	}
	return &BlobStore{max: max, values: map[string][]byte{}}, nil
}
func (b *BlobStore) Put(ref contract.BlobRef, value []byte) error {
	if b == nil {
		return ErrBlobNotFound
	}
	if ref.Size != len(value) || ref.Size > b.max {
		return contract.ErrLimitExceeded
	}
	if contract.CanonicalDigest(value) != ref.Digest {
		return fmt.Errorf("%w: digest mismatch", ErrBlobNotFound)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if old, ok := b.values[ref.Digest]; ok {
		if len(old) == len(value) {
			return nil
		}
	}
	if b.used+len(value) > b.max {
		return contract.ErrLimitExceeded
	}
	b.values[ref.Digest] = append([]byte(nil), value...)
	b.used += len(value)
	return nil
}
func (b *BlobStore) Get(ref contract.BlobRef) ([]byte, error) {
	if b == nil {
		return nil, ErrBlobNotFound
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	value, ok := b.values[ref.Digest]
	if !ok {
		return nil, ErrBlobNotFound
	}
	return append([]byte(nil), value...), nil
}
