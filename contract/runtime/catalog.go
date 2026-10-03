package runtime

import (
	"encoding/json"
	"errors"
	artifactcontract "github.com/well-prado/new-blok/contract/artifact"
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
		if err := node.ValidateDescriptor(d); err != nil {
			return "", err
		}
		if i > 0 && ds[i-1].Name == d.Name && ds[i-1].Version == d.Version {
			return "", errors.New("duplicate catalog identity")
		}
		ds[i].Effects = append([]string(nil), d.Effects...)
		sort.Strings(ds[i].Effects)
		ds[i].RequiredCapabilities = append([]string(nil), d.RequiredCapabilities...)
		sort.Strings(ds[i].RequiredCapabilities)
	}
	raw, err := json.Marshal(ds)
	if err != nil {
		return "", err
	}
	canonical, err := artifactcontract.CanonicalJSON(raw)
	if err != nil {
		return "", err
	}
	return CanonicalDigest(canonical), nil
}
