package runtime

import (
	"encoding/json"
	"errors"
	"github.com/well-prado/new-blok/contract/artifact"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/node"
	"sort"
)

const MaxCatalogNodes = 1024

// CatalogDigest canonicalizes every JSON object key and sorts node identities.
// Both peers hash these exact bytes, independently of discovery order.
func CatalogDigest(descriptors []node.Descriptor) (string, error) {
	if len(descriptors) == 0 || len(descriptors) > MaxCatalogNodes {
		return "", ErrLimitExceeded
	}
	ds := append([]node.Descriptor(nil), descriptors...)
	sort.Slice(ds, func(i, j int) bool { return ds[i].Name+"@"+ds[i].Version < ds[j].Name+"@"+ds[j].Version })
	for i, d := range ds {
		if !identityPattern.MatchString(d.Name) || d.Version == "" || d.Description == "" {
			return "", errors.New("invalid catalog descriptor")
		}
		if i > 0 && ds[i-1].Name == d.Name && ds[i-1].Version == d.Version {
			return "", errors.New("duplicate catalog identity")
		}
		if _, err := schema.Parse(d.InputSchema); err != nil {
			return "", err
		}
		if _, err := schema.Parse(d.OutputSchema); err != nil {
			return "", err
		}
		ds[i].Effects = append([]string(nil), d.Effects...)
		sort.Strings(ds[i].Effects)
	}
	raw, err := json.Marshal(ds)
	if err != nil {
		return "", err
	}
	canonical, err := artifact.CanonicalJSON(raw)
	if err != nil {
		return "", err
	}
	return CanonicalDigest(canonical), nil
}
