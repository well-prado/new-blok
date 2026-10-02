package runtime

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"regexp"
	"strings"
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
type TokenAuthenticator struct {
	mu      sync.RWMutex
	tokens  []storedToken
	revoked map[[32]byte]bool
}
type storedToken struct {
	digest    [32]byte
	principal Principal
}

func NewTokenAuthenticator(values []Credential) (*TokenAuthenticator, error) {
	out := &TokenAuthenticator{revoked: make(map[[32]byte]bool)}
	for _, value := range values {
		if value.Token == "" || len(value.Token) > 4096 || !authIdentity.MatchString(value.Principal) || !validCapabilities(value.Capabilities) {
			return nil, ErrUnauthenticated
		}
		digest := sha256.Sum256([]byte(value.Token))
		for _, previous := range out.tokens {
			if previous.digest == digest {
				return nil, ErrUnauthenticated
			}
		}
		out.tokens = append(out.tokens, storedToken{digest: digest, principal: Principal{Name: value.Principal, Capabilities: contract.SortedCapabilities(value.Capabilities)}})
	}
	if len(out.tokens) == 0 {
		return nil, ErrUnauthenticated
	}
	return out, nil
}
func (a *TokenAuthenticator) Authenticate(token string, requested []contract.Capability) (Principal, error) {
	if a == nil || token == "" || len(token) > 4096 {
		return Principal{}, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(token))
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.revoked[digest] {
		return Principal{}, ErrUnauthenticated
	}
	if !validCapabilities(requested) {
		return Principal{}, ErrUnauthorized
	}
	for _, candidate := range a.tokens {
		if subtle.ConstantTimeCompare(digest[:], candidate.digest[:]) == 1 {
			if !containsCapabilities(candidate.principal.Capabilities, requested) {
				return Principal{}, ErrUnauthorized
			}
			return Principal{Name: candidate.principal.Name, Capabilities: contract.SortedCapabilities(requested)}, nil
		}
	}
	return Principal{}, ErrUnauthenticated
}

var authIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
var blobDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func validCapabilities(values []contract.Capability) bool {
	if len(values) > 128 {
		return false
	}
	for _, value := range values {
		if !authIdentity.MatchString(string(value)) || strings.Contains(string(value), "orchestrate") {
			return false
		}
	}
	return true
}

// Revoke permanently invalidates a credential, including existing sessions.
func (a *TokenAuthenticator) Revoke(token string) {
	if a == nil {
		return
	}
	digest := sha256.Sum256([]byte(token))
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, candidate := range a.tokens {
		if candidate.digest == digest {
			a.revoked[digest] = true
			return
		}
	}
}

// AuthenticatedSession is authority minted by the authenticator. A caller's
// Principal value or JSON principal field is never a substitute for this proof.
type AuthenticatedSession struct {
	mu        sync.RWMutex
	auth      *TokenAuthenticator
	digest    [32]byte
	principal Principal
}

func (a *TokenAuthenticator) NewSession(token, claimedPrincipal string, requested []contract.Capability) (*AuthenticatedSession, error) {
	p, err := a.Authenticate(token, requested)
	if err != nil {
		return nil, err
	}
	if claimedPrincipal != p.Name {
		return nil, ErrUnauthorized
	}
	return &AuthenticatedSession{auth: a, digest: sha256.Sum256([]byte(token)), principal: p}, nil
}

func (s *AuthenticatedSession) Principal() Principal {
	if s == nil {
		return Principal{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Principal{Name: s.principal.Name, Capabilities: append([]contract.Capability(nil), s.principal.Capabilities...)}
}

func (s *AuthenticatedSession) Authorize(required []contract.Capability) error {
	if s == nil {
		return ErrUnauthenticated
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authorizeLocked(required)
}

// ValidateCall checks caller assertions against the established transport grant.
// Node-required capabilities must additionally pass Authorize before dispatch.
func (s *AuthenticatedSession) ValidateCall(principal string, capabilities []contract.Capability) error {
	if s == nil {
		return ErrUnauthenticated
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.authorizeLocked(capabilities); err != nil {
		return err
	}
	if principal != s.principal.Name {
		return ErrUnauthorized
	}
	return nil
}

func (s *AuthenticatedSession) authorizeLocked(required []contract.Capability) error {
	if s.auth == nil {
		return ErrUnauthenticated
	}
	s.auth.mu.RLock()
	defer s.auth.mu.RUnlock()
	if s.auth.revoked[s.digest] {
		return ErrUnauthenticated
	}
	if !validCapabilities(required) || !containsCapabilities(s.principal.Capabilities, required) {
		return ErrUnauthorized
	}
	return nil
}

// Reconnect retains credential identity and may only narrow the previous grant.
// A replacement credential requires a new session, not an implicit reconnect.
func (s *AuthenticatedSession) Reconnect(token, claimedPrincipal string, requested []contract.Capability) error {
	if s == nil {
		return ErrUnauthenticated
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.authorizeLocked(requested); err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(digest[:], s.digest[:]) != 1 {
		return ErrUnauthenticated
	}
	p, err := s.auth.Authenticate(token, requested)
	if err != nil {
		return err
	}
	if claimedPrincipal != s.principal.Name || p.Name != s.principal.Name {
		return ErrUnauthorized
	}
	s.principal = p
	return nil
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
	values map[blobKey][]byte
	used   int
}

type blobKey struct {
	owner      *TokenAuthenticator
	credential [32]byte
	digest     string
}

func NewBlobStore(max int) (*BlobStore, error) {
	if max <= 0 {
		return nil, contract.ErrLimitExceeded
	}
	return &BlobStore{max: max, values: map[blobKey][]byte{}}, nil
}

// Put and Get are trusted in-process access only. Transport adapters must use
// PutAuthorized/GetAuthorized; their namespace cannot read trusted-local blobs.
func (b *BlobStore) Put(ref contract.BlobRef, value []byte) error {
	return b.put(blobKey{digest: ref.Digest}, ref, value)
}
func (b *BlobStore) PutAuthorized(s *AuthenticatedSession, ref contract.BlobRef, value []byte) error {
	if err := s.Authorize([]contract.Capability{"blob:write"}); err != nil {
		return err
	}
	return b.put(blobKey{owner: s.auth, credential: s.digest, digest: ref.Digest}, ref, value)
}
func (b *BlobStore) put(key blobKey, ref contract.BlobRef, value []byte) error {
	if b == nil {
		return ErrBlobNotFound
	}
	if ref.Size < 0 || ref.Size != len(value) || ref.Size > b.max || ref.Size > contract.MaxBlobBytes || !blobDigest.MatchString(ref.Digest) {
		return contract.ErrLimitExceeded
	}
	if contract.CanonicalDigest(value) != ref.Digest {
		return fmt.Errorf("%w: digest mismatch", ErrBlobNotFound)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if old, ok := b.values[key]; ok {
		if len(old) == len(value) {
			return nil
		}
	}
	// Bound metadata too: zero-byte blobs and many owners must not bypass bytes.
	if len(b.values) >= 1024 || len(value) > b.max-b.used {
		return contract.ErrLimitExceeded
	}
	b.values[key] = append([]byte(nil), value...)
	b.used += len(value)
	return nil
}
func (b *BlobStore) Get(ref contract.BlobRef) ([]byte, error) {
	return b.get(blobKey{digest: ref.Digest}, ref)
}
func (b *BlobStore) GetAuthorized(s *AuthenticatedSession, ref contract.BlobRef) ([]byte, error) {
	if err := s.Authorize([]contract.Capability{"blob:read"}); err != nil {
		return nil, err
	}
	return b.get(blobKey{owner: s.auth, credential: s.digest, digest: ref.Digest}, ref)
}

// ResolveAuthorized validates the whole reference list before allocating any
// payload. Repeated references count toward the aggregate returned byte limit.
func (b *BlobStore) ResolveAuthorized(s *AuthenticatedSession, refs []contract.BlobRef, maxBytes int) ([][]byte, error) {
	if err := s.Authorize([]contract.Capability{"blob:read"}); err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrBlobNotFound
	}
	if maxBytes < 1 || maxBytes > contract.MaxBlobBytes || len(refs) > 1024 {
		return nil, contract.ErrLimitExceeded
	}
	total := 0
	for _, ref := range refs {
		if ref.Size < 0 || ref.Size > maxBytes-total || !blobDigest.MatchString(ref.Digest) {
			return nil, contract.ErrLimitExceeded
		}
		total += ref.Size
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ref := range refs {
		key := blobKey{owner: s.auth, credential: s.digest, digest: ref.Digest}
		if value, ok := b.values[key]; !ok || len(value) != ref.Size {
			return nil, ErrBlobNotFound
		}
	}
	out := make([][]byte, len(refs))
	for i, ref := range refs {
		out[i] = append([]byte(nil), b.values[blobKey{owner: s.auth, credential: s.digest, digest: ref.Digest}]...)
	}
	return out, nil
}
func (b *BlobStore) get(key blobKey, ref contract.BlobRef) ([]byte, error) {
	if b == nil {
		return nil, ErrBlobNotFound
	}
	if ref.Size < 0 || ref.Size > b.max || ref.Size > contract.MaxBlobBytes || !blobDigest.MatchString(ref.Digest) {
		return nil, contract.ErrLimitExceeded
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	value, ok := b.values[key]
	if !ok || len(value) != ref.Size {
		return nil, ErrBlobNotFound
	}
	return append([]byte(nil), value...), nil
}
