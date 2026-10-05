package cluster

import (
	"encoding/json"
	"os"
	"testing"
)

func TestStableTenantPartitionAndFairSelectionFixture(t *testing.T) {
	data, err := os.ReadFile("../../testdata/distributed/runtime-admission-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		PartitionCount int      `json:"partitionCount"`
		Tenant         string   `json:"tenant"`
		Expected       string   `json:"expectedPartition"`
		Tenants        []string `json:"candidateTenants"`
		ExpectedOrder  []string `json:"expectedFairOrder"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{limits: Limits{Partitions: fixture.PartitionCount}}
	if got := runtime.Partition(fixture.Tenant); got != fixture.Expected || runtime.Partition(fixture.Tenant) != got {
		t.Fatalf("stable partition=%q, expected %q", got, fixture.Expected)
	}
	lastTenant := ""
	for index, expectedTenant := range fixture.ExpectedOrder {
		records := make([]RunRecord, 0, len(fixture.Tenants))
		for position, tenant := range fixture.Tenants {
			records = append(records, RunRecord{RunID: string(rune('a' + position)), Tenant: tenant})
		}
		got := nextFairCandidate(lastTenant, records).Tenant
		if got != expectedTenant {
			t.Fatalf("fair turn %d=%q, expected %q", index, got, expectedTenant)
		}
		lastTenant = got
	}
}
