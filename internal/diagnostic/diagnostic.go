// Package diagnostic defines stable machine-readable validation failures.
package diagnostic

import (
	"encoding/json"
	"fmt"
	"sort"
)

type Diagnostic struct {
	Code        string `json:"code"`
	Source      string `json:"source,omitempty"`
	Step        string `json:"step,omitempty"`
	Field       string `json:"field,omitempty"`
	Expected    string `json:"expected,omitempty"`
	Actual      string `json:"actual,omitempty"`
	Remediation string `json:"remediation,omitempty"`
	Message     string `json:"message"`
}

func (d Diagnostic) Error() string { return fmt.Sprintf("%s: %s", d.Code, d.Message) }

func (d Diagnostic) Validate() error {
	if d.Code == "" || d.Message == "" {
		return fmt.Errorf("diagnostic requires code and message")
	}
	if d.Remediation == "" {
		return fmt.Errorf("diagnostic %s requires remediation", d.Code)
	}
	return nil
}

func (d Diagnostic) JSON() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(d)
}

func Sort(items []Diagnostic) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Code != items[j].Code {
			return items[i].Code < items[j].Code
		}
		if items[i].Source != items[j].Source {
			return items[i].Source < items[j].Source
		}
		return items[i].Step < items[j].Step
	})
}
