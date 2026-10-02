// Package conformance runs versioned semantic cases through narrow adapter
// ports. It does not import or execute the internal workflow engine.
package conformance

import (
	"context"
	"fmt"
	"sort"
)

type Case struct {
	ID              string `json:"id"`
	Kind            string `json:"kind"`
	ExpectedOutput  int    `json:"expectedOutput"`
	ExpectedErrors  int    `json:"expectedErrors"`
	ExpectedEffects int    `json:"expectedEffects"`
	Unsupported     bool   `json:"unsupported"`
}

type Result struct {
	CaseID      string `json:"caseId"`
	Output      int    `json:"output"`
	Errors      int    `json:"errors"`
	Effects     int    `json:"effects"`
	Unsupported bool   `json:"unsupported"`
	Reason      string `json:"reason,omitempty"`
}

type Adapter interface {
	Name() string
	Run(context.Context, Case) (Result, error)
}

type Report struct {
	Adapter string   `json:"adapter"`
	Results []Result `json:"results"`
}

func Run(ctx context.Context, adapter Adapter, cases []Case) (Report, error) {
	if adapter == nil {
		return Report{}, fmt.Errorf("adapter is required")
	}
	ordered := append([]Case(nil), cases...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	report := Report{Adapter: adapter.Name()}
	for _, c := range ordered {
		result, err := adapter.Run(ctx, c)
		if err != nil {
			return report, fmt.Errorf("case %s: %w", c.ID, err)
		}
		if result.CaseID == "" {
			result.CaseID = c.ID
		}
		if c.Unsupported && !result.Unsupported {
			return report, fmt.Errorf("case %s must report unsupported", c.ID)
		}
		if !c.Unsupported && result.Unsupported {
			return report, fmt.Errorf("case %s unexpectedly unsupported", c.ID)
		}
		if !result.Unsupported && (result.Output != c.ExpectedOutput || result.Errors != c.ExpectedErrors || result.Effects != c.ExpectedEffects) {
			return report, fmt.Errorf("case %s result mismatch: got %d/%d/%d want %d/%d/%d", c.ID, result.Output, result.Errors, result.Effects, c.ExpectedOutput, c.ExpectedErrors, c.ExpectedEffects)
		}
		report.Results = append(report.Results, result)
	}
	return report, nil
}
