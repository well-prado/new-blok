package packagecontract

import (
	"sync"

	"github.com/well-prado/new-blok/contract"
)

const (
	DefaultStoreMaxPackages      = 256
	DefaultStoreMaxArtifactBytes = 128 << 20
	HardStoreMaxPackages         = 4096
	HardStoreMaxArtifactBytes    = 1 << 30
)

// StoreLimits bounds retained package count and aggregate artifact bytes. Zero
// selects that field's default. Configured values cannot exceed the hard caps.
type StoreLimits struct {
	MaxPackages      int
	MaxArtifactBytes int64
}

// Store is an in-memory, offline package source useful to local consumers and
// conformance adapters. It never executes package contents and bounds retained
// entries and artifact bytes.
type Store struct {
	mu            sync.RWMutex
	packages      map[string]Bundle
	artifactBytes int64
	limits        StoreLimits
}

func NewStore() *Store {
	store, _ := NewStoreWithLimits(StoreLimits{})
	return store
}

func NewStoreWithLimits(limits StoreLimits) (*Store, error) {
	if limits.MaxPackages == 0 {
		limits.MaxPackages = DefaultStoreMaxPackages
	}
	if limits.MaxArtifactBytes == 0 {
		limits.MaxArtifactBytes = DefaultStoreMaxArtifactBytes
	}
	if limits.MaxPackages < 1 || limits.MaxPackages > HardStoreMaxPackages || limits.MaxArtifactBytes < 1 || limits.MaxArtifactBytes > HardStoreMaxArtifactBytes {
		return nil, &Error{Code: "invalid_store_limits", Message: "store limits must be positive and within the 4096 package and 1 GiB hard caps"}
	}
	return &Store{packages: make(map[string]Bundle), limits: limits}, nil
}

func (s *Store) Limits() StoreLimits {
	if s == nil {
		return StoreLimits{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.limits
}

func (s *Store) Usage() (packages int, artifactBytes int64) {
	if s == nil {
		return 0, 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.packages), s.artifactBytes
}

func (s *Store) Publish(bundle Bundle, policy TrustPolicy, env Environment) (Verified, error) {
	if s == nil {
		return Verified{}, &Error{Code: "invalid_manifest", Message: "package store is nil"}
	}
	if err := validateArtifactSize(bundle.Artifact); err != nil {
		return Verified{}, err
	}
	if err := bundle.Manifest.Validate(); err != nil {
		return Verified{}, err
	}
	bundle = cloneBundle(bundle)
	verified, err := bundle.Verify(policy, env)
	if err != nil {
		return Verified{}, err
	}
	key := packageKey(bundle.Manifest.Identity)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, exists := s.packages[key]; exists {
		oldDigest, oldErr := old.Manifest.Digest()
		if oldErr != nil || oldDigest != verified.ManifestDigest || ArtifactDigest(old.Artifact) != verified.ArtifactDigest || !sameSignature(old.Signature, bundle.Signature) {
			return Verified{}, &Error{Code: "version_conflict", Path: "identity", Message: "an immutable package version already exists with different content"}
		}
		return verified, nil
	}
	if len(s.packages) >= s.limits.MaxPackages {
		return Verified{}, &Error{Code: "store_capacity_exceeded", Path: "store", Message: "local package count limit reached"}
	}
	artifactSize := int64(len(bundle.Artifact))
	if artifactSize > s.limits.MaxArtifactBytes-s.artifactBytes {
		return Verified{}, &Error{Code: "store_capacity_exceeded", Path: "store", Message: "local retained artifact byte limit reached"}
	}
	s.packages[key] = cloneBundle(bundle)
	s.artifactBytes += artifactSize
	return verified, nil
}

func (s *Store) Fetch(id Identity, policy TrustPolicy, env Environment) (Bundle, Verified, error) {
	if s == nil {
		return Bundle{}, Verified{}, ErrNotFound
	}
	if err := id.Validate(); err != nil {
		return Bundle{}, Verified{}, err
	}
	s.mu.RLock()
	bundle, ok := s.packages[packageKey(id)]
	s.mu.RUnlock()
	if !ok {
		return Bundle{}, Verified{}, ErrNotFound
	}
	bundle = cloneBundle(bundle)
	verified, err := bundle.Verify(policy, env)
	if err != nil {
		return Bundle{}, Verified{}, err
	}
	return bundle, verified, nil
}

func packageKey(id Identity) string { return id.Name + "@" + id.Version }

func cloneBundle(bundle Bundle) Bundle {
	bundle.Artifact = append([]byte(nil), bundle.Artifact...)
	if bundle.Signature != nil {
		signature := *bundle.Signature
		bundle.Signature = &signature
	}
	bundle.Manifest.Dependencies = append([]Dependency(nil), bundle.Manifest.Dependencies...)
	if bundle.Manifest.Compatibility.Runtimes != nil {
		runtimes := make(map[string]string, len(bundle.Manifest.Compatibility.Runtimes))
		for key, value := range bundle.Manifest.Compatibility.Runtimes {
			runtimes[key] = value
		}
		bundle.Manifest.Compatibility.Runtimes = runtimes
	}
	metadata := &bundle.Manifest.Metadata
	if metadata.CapabilityManifest != nil {
		capability := *metadata.CapabilityManifest
		capability.Effects = append([]string(nil), capability.Effects...)
		capability.Capabilities = append([]string(nil), capability.Capabilities...)
		capability.SecretRefs = append([]string(nil), capability.SecretRefs...)
		metadata.CapabilityManifest = &capability
	}
	if metadata.NodeDescriptor != nil {
		descriptor := *metadata.NodeDescriptor
		descriptor.InputSchema = append([]byte(nil), descriptor.InputSchema...)
		descriptor.OutputSchema = append([]byte(nil), descriptor.OutputSchema...)
		descriptor.Effects = append([]string(nil), descriptor.Effects...)
		descriptor.RequiredCapabilities = append([]string(nil), descriptor.RequiredCapabilities...)
		metadata.NodeDescriptor = &descriptor
	}
	if metadata.WorkflowDocument != nil {
		document := *metadata.WorkflowDocument
		document.Workflow.InputSchema = append([]byte(nil), document.Workflow.InputSchema...)
		document.Workflow.OutputSchema = append([]byte(nil), document.Workflow.OutputSchema...)
		document.Nodes = append([]contract.NodeDescriptor(nil), document.Nodes...)
		for i := range document.Nodes {
			document.Nodes[i].InputSchema = append([]byte(nil), document.Nodes[i].InputSchema...)
			document.Nodes[i].OutputSchema = append([]byte(nil), document.Nodes[i].OutputSchema...)
		}
		document.Bindings = append([]contract.Binding(nil), document.Bindings...)
		for i := range document.Bindings {
			document.Bindings[i].InputSchema = append([]byte(nil), document.Bindings[i].InputSchema...)
			document.Bindings[i].Source = cloneSourceSpan(document.Bindings[i].Source)
		}
		document.Workflow.Instructions = append([]contract.Instruction(nil), document.Workflow.Instructions...)
		for i := range document.Workflow.Instructions {
			document.Workflow.Instructions[i].References = append([]contract.Reference(nil), document.Workflow.Instructions[i].References...)
			for j := range document.Workflow.Instructions[i].References {
				document.Workflow.Instructions[i].References[j].Path = append([]string(nil), document.Workflow.Instructions[i].References[j].Path...)
			}
			document.Workflow.Instructions[i].Source = cloneSourceSpan(document.Workflow.Instructions[i].Source)
		}
		metadata.WorkflowDocument = &document
	}
	return bundle
}

func cloneSourceSpan(span *contract.SourceSpan) *contract.SourceSpan {
	if span == nil {
		return nil
	}
	copy := *span
	return &copy
}

func sameSignature(a, b *Signature) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
