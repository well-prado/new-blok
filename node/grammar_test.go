package node

import (
	"context"
	"strings"
	"testing"
)

// The validation grammars are compiled once at package level (#319). These
// tables pin the accepted and rejected sets so hoisting a pattern cannot
// change what Define accepts.
func TestDefineGrammarTable(t *testing.T) {
	long := func(prefix string, n int) string { return prefix + strings.Repeat("a", n-len(prefix)) }
	tests := []struct {
		label, name, version string
		caps                 []string
		code                 string // "" means accepted
	}{
		{"plain name", "shop/quote", "1.0.0", nil, ""},
		{"name with separators", "a_b-c/d", "10.20.30", nil, ""},
		{"single letter name", "a", "0.0.0", nil, ""},
		{"128 byte name", long("a", 128), "1.0.0", nil, ""},
		{"129 byte name", long("a", 129), "1.0.0", nil, "invalid_node_identity"},
		{"empty name", "", "1.0.0", nil, "invalid_node_identity"},
		{"upper case name", "Shop/quote", "1.0.0", nil, "invalid_node_identity"},
		{"digit first name", "1shop", "1.0.0", nil, "invalid_node_identity"},
		{"slash first name", "/shop", "1.0.0", nil, "invalid_node_identity"},
		{"dot in name", "shop.quote", "1.0.0", nil, "invalid_node_identity"},
		{"dot dot in name", "shop/../quote", "1.0.0", nil, "invalid_node_identity"},
		{"space in name", "shop quote", "1.0.0", nil, "invalid_node_identity"},
		{"trailing newline name", "shop\n", "1.0.0", nil, "invalid_node_identity"},
		{"non ascii name", "shöp", "1.0.0", nil, "invalid_node_identity"},
		{"empty version", "shop", "", nil, "invalid_node_version"},
		{"two part version", "shop", "1.0", nil, "invalid_node_version"},
		{"four part version", "shop", "1.0.0.0", nil, "invalid_node_version"},
		{"v prefix version", "shop", "v1.0.0", nil, "invalid_node_version"},
		{"prerelease version", "shop", "1.0.0-rc1", nil, "invalid_node_version"},
		{"trailing newline version", "shop", "1.0.0\n", nil, "invalid_node_version"},
		{"leading space version", "shop", " 1.0.0", nil, "invalid_node_version"},
		{"leading zero version", "shop", "01.002.0003", nil, ""},
		{"capability plain", "shop", "1.0.0", []string{"orders:read"}, ""},
		{"capability mixed", "shop", "1.0.0", []string{"A", "b.c", "d_e", "f-g", "h/i", "j:k", "9"}, ""},
		{"capability 128 bytes", "shop", "1.0.0", []string{long("A", 128)}, ""},
		{"capability 129 bytes", "shop", "1.0.0", []string{long("A", 129)}, "invalid_capability"},
		{"capability empty", "shop", "1.0.0", []string{""}, "invalid_capability"},
		{"capability leading dot", "shop", "1.0.0", []string{".a"}, "invalid_capability"},
		{"capability leading dash", "shop", "1.0.0", []string{"-a"}, "invalid_capability"},
		{"capability space", "shop", "1.0.0", []string{"a b"}, "invalid_capability"},
		{"capability newline", "shop", "1.0.0", []string{"a\n"}, "invalid_capability"},
		{"capability orchestrate", "shop", "1.0.0", []string{"x:orchestrate"}, "invalid_capability"},
		{"capability duplicate", "shop", "1.0.0", []string{"a", "a"}, "invalid_capability"},
		{"capability non ascii", "shop", "1.0.0", []string{"é"}, "invalid_capability"},
		{"capability one bad of many", "shop", "1.0.0", []string{"a", "b", "c d"}, "invalid_capability"},
	}
	handler := func(context.Context, quoteInput) (quoteOutput, error) { return quoteOutput{}, nil }
	for _, tc := range tests {
		t.Run(tc.label, func(t *testing.T) {
			_, err := Define(tc.name, tc.version, handler, Description("d"), Schemas(quoteSchemas, quoteSchemas), RequiredCapabilities(tc.caps...))
			got := ""
			if err != nil {
				e, ok := err.(*Error)
				if !ok {
					t.Fatalf("error %T %v; want *Error", err, err)
				}
				got = e.Code
			}
			if got != tc.code {
				t.Fatalf("code %q; want %q", got, tc.code)
			}
		})
	}
}

func BenchmarkDefine(b *testing.B) {
	handler := func(context.Context, quoteInput) (quoteOutput, error) { return quoteOutput{}, nil }
	options := []Option{Description("calculates a quote"), Schemas(quoteSchemas, quoteSchemas), RequiredCapabilities("orders:read", "orders:write")}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Define("shop/calculate-quote", "1.0.0", handler, options...); err != nil {
			b.Fatal(err)
		}
	}
}
