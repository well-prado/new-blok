// Package fixture is the generator's executable conformance fixture (#240):
// bindings_gen.go is generate.Source applied to this file, and a real
// workflow composes values through those generated accessors.
package fixture

// Line names its JSON keys in every way encoding/json allows.
type Line struct {
	SKU        string `json:"sku"`
	TotalCents int64
	Total      int64  `json:"total_cents,omitempty"`
	Secret     string `json:"-"`
}
