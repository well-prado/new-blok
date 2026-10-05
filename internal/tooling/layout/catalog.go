package layout

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// CatalogVersion is the version of the Catalog document.
const CatalogVersion = 1

// Catalog is the layout-independent view of a Project: identities, runtimes
// and node-relative ownership. It never holds a project path, a directory
// name or the layout, so the classic and unified forms of one application
// encode to the same bytes and digest.
type Catalog struct {
	Version   int               `json:"version"`
	Nodes     []CatalogNode     `json:"nodes"`
	Workflows []CatalogWorkflow `json:"workflows"`
}

type CatalogNode struct {
	Name    string   `json:"name"`
	Version string   `json:"version"`
	Runtime string   `json:"runtime"`
	Files   []string `json:"files"`
}

type CatalogWorkflow struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Catalog returns the canonical catalog: nodes and workflows sorted by
// name and version, each node's files sorted.
func (p *Project) Catalog() Catalog {
	catalog := Catalog{Version: CatalogVersion, Nodes: []CatalogNode{}, Workflows: []CatalogWorkflow{}}
	for _, node := range p.Nodes {
		files := append([]string{}, node.Files...)
		sort.Strings(files)
		catalog.Nodes = append(catalog.Nodes, CatalogNode{Name: node.Name, Version: node.Version, Runtime: node.Runtime, Files: files})
	}
	for _, workflow := range p.Workflows {
		catalog.Workflows = append(catalog.Workflows, CatalogWorkflow{Name: workflow.Name, Version: workflow.Version})
	}
	sort.Slice(catalog.Nodes, func(i, j int) bool {
		a, b := catalog.Nodes[i], catalog.Nodes[j]
		return a.Name+"@"+a.Version < b.Name+"@"+b.Version
	})
	sort.Slice(catalog.Workflows, func(i, j int) bool {
		a, b := catalog.Workflows[i], catalog.Workflows[j]
		return a.Name+"@"+a.Version < b.Name+"@"+b.Version
	})
	return catalog
}

// Canonical is the catalog's canonical encoding: compact JSON of the sorted
// document. Struct field order fixes key order.
func (c Catalog) Canonical() ([]byte, error) { return json.Marshal(c) }

// Digest is "sha256:" and the hex SHA-256 of Canonical.
func (c Catalog) Digest() (string, error) {
	canonical, err := c.Canonical()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
