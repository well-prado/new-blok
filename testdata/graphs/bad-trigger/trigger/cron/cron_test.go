package cron

import (
	"testing"

	conf "fixture.test/contract/conformance"
)

func TestConformance(t *testing.T) { conf.RunTrigger() }
