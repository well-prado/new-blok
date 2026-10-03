package mcp

import (
	"testing"

	"fixture.test/contract/conformance"
)

func TestConformance(t *testing.T) { conformance.RunTrigger() }
