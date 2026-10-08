package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// RootIteration is the iteration path of a step outside every loop
// (ADR 0028). A StepIdentity may also spell it as the empty string; both
// are the same iteration and have the same OperationKey.
const RootIteration = "root"

// NewStepIdentity is the identity of step stepID of a run in the given
// iteration, for its resolved input (or, for a wait, its wait plan).
func NewStepIdentity(runID, artifactDigest, stepID, iterationPath string, input []byte) StepIdentity {
	inputHash := sha256.Sum256(input)
	identity := StepIdentity{RunID: runID, ArtifactDigest: artifactDigest, StepID: stepID, InputDigest: "sha256:" + hex.EncodeToString(inputHash[:]), IterationPath: iterationPath}
	identity.OperationKey = OperationKey(identity)
	return identity
}

// Iteration is i's iteration path, RootIteration when it is empty.
func (i StepIdentity) Iteration() string {
	if i.IterationPath == "" {
		return RootIteration
	}
	return i.IterationPath
}

// Invocation is i's invocation path: InvocationPath inside an arm, the
// step id at the top level.
func (i StepIdentity) Invocation() string {
	if i.InvocationPath == "" {
		return i.StepID
	}
	return i.InvocationPath
}

// Canonical is i with the root iteration, and a top-level invocation path,
// spelled as the empty string, the form stored records compare in: a
// record written before IterationPath or InvocationPath existed decodes
// with them empty.
func (i StepIdentity) Canonical() StepIdentity {
	if i.IterationPath == RootIteration {
		i.IterationPath = ""
	}
	if i.InvocationPath == i.StepID {
		i.InvocationPath = ""
	}
	return i
}

// operationKeyV1 is the encoding OperationKey hashes. Its bytes for a
// root-iteration step are exactly those json.Marshal produced for the
// untagged StepIdentity before #382 (the empty OperationKey included), so
// every key already stored stays valid; other iterations add
// IterationPath. It is separate from StepIdentity so that no change to
// that struct can move a key: a new input to the key needs a new,
// explicitly versioned encoding (ADR 0027, "Identity encoding").
type operationKeyV1 struct {
	RunID          string `json:"RunID"`
	ArtifactDigest string `json:"ArtifactDigest"`
	StepID         string `json:"StepID"`
	InputDigest    string `json:"InputDigest"`
	OperationKey   string `json:"OperationKey"`
	IterationPath  string `json:"IterationPath,omitempty"`
}

// OperationKey is the stable key of i's step execution: equal for every
// retry and replay of the step in the same iteration, different in
// another iteration. i.OperationKey itself is ignored.
func OperationKey(i StepIdentity) string {
	canonical := i.Canonical()
	encoded, _ := json.Marshal(operationKeyV1{RunID: canonical.RunID, ArtifactDigest: canonical.ArtifactDigest, StepID: canonical.StepID, InputDigest: canonical.InputDigest, IterationPath: canonical.IterationPath})
	hash := sha256.Sum256(encoded)
	return "op:" + hex.EncodeToString(hash[:])
}

// SameExecution reports whether i and o identify the same step execution,
// spelling the root iteration either way.
func (i StepIdentity) SameExecution(o StepIdentity) bool {
	return i.Canonical() == o.Canonical()
}
