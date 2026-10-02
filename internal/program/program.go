// Package program owns immutable, versioned compiled programs.
package program

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/well-prado/new-blok/contract"
)

const (
	Version           = 1
	CheckpointVersion = 1
	DefaultMaxSteps   = 10000
	DefaultMaxDepth   = 64
)

type Limits struct {
	MaxSteps int
	MaxDepth int
}

type Program struct {
	workflowID   string
	version      int
	checkpoint   int
	digest       string
	instructions []contract.InternalInstruction
}

type wire struct {
	Version           int                            `json:"version"`
	CheckpointVersion int                            `json:"checkpointVersion"`
	WorkflowID        string                         `json:"workflowId"`
	Instructions      []contract.InternalInstruction `json:"instructions"`
	Digest            string                         `json:"digest"`
}

func Build(input contract.InternalProgram, limits Limits) (Program, error) {
	if limits.MaxSteps == 0 {
		limits.MaxSteps = DefaultMaxSteps
	}
	if limits.MaxDepth == 0 {
		limits.MaxDepth = DefaultMaxDepth
	}
	if limits.MaxSteps < 1 || limits.MaxDepth < 1 {
		return Program{}, fmt.Errorf("invalid program limits")
	}
	if len(input.Instructions) > limits.MaxSteps {
		return Program{}, fmt.Errorf("program_limits: instruction count exceeds %d", limits.MaxSteps)
	}
	if err := validateInstructions(input.Instructions, limits.MaxDepth); err != nil {
		return Program{}, err
	}
	instructions := cloneInstructions(input.Instructions)
	sort.SliceStable(instructions, func(i, j int) bool { return instructions[i].Index < instructions[j].Index })
	value := wire{Version: Version, CheckpointVersion: CheckpointVersion, WorkflowID: input.WorkflowID, Instructions: instructions}
	digest, err := digestWire(value)
	if err != nil {
		return Program{}, err
	}
	return Program{workflowID: input.WorkflowID, version: Version, checkpoint: CheckpointVersion, digest: digest, instructions: instructions}, nil
}

func Decode(data []byte) (Program, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var value wire
	if err := decoder.Decode(&value); err != nil {
		return Program{}, fmt.Errorf("invalid_program: %w", err)
	}
	if value.Version != Version {
		return Program{}, fmt.Errorf("unsupported_program_version: %d", value.Version)
	}
	if value.CheckpointVersion != CheckpointVersion {
		return Program{}, fmt.Errorf("unsupported_checkpoint_version: %d", value.CheckpointVersion)
	}
	if err := validateInstructions(value.Instructions, DefaultMaxDepth); err != nil {
		return Program{}, err
	}
	expected, err := digestWire(wire{Version: value.Version, CheckpointVersion: value.CheckpointVersion, WorkflowID: value.WorkflowID, Instructions: value.Instructions})
	if err != nil {
		return Program{}, err
	}
	if value.Digest != expected {
		return Program{}, fmt.Errorf("program_digest_mismatch: expected %s, got %s", expected, value.Digest)
	}
	return Program{workflowID: value.WorkflowID, version: value.Version, checkpoint: value.CheckpointVersion, digest: value.Digest, instructions: cloneInstructions(value.Instructions)}, nil
}

func (p Program) WorkflowID() string     { return p.workflowID }
func (p Program) Version() int           { return p.version }
func (p Program) CheckpointVersion() int { return p.checkpoint }
func (p Program) Digest() string         { return p.digest }

func (p Program) Instructions() []contract.InternalInstruction {
	return cloneInstructions(p.instructions)
}

func (p Program) JSON() ([]byte, error) {
	return json.Marshal(wire{Version: p.version, CheckpointVersion: p.checkpoint, WorkflowID: p.workflowID, Instructions: cloneInstructions(p.instructions), Digest: p.digest})
}

func validateInstructions(instructions []contract.InternalInstruction, maxDepth int) error {
	known := map[string]bool{"call": true, "condition": true, "output": true, "wait": true, "parallel": true}
	seen := map[string]bool{}
	depths := map[string]int{}
	for index, instruction := range instructions {
		if !known[instruction.Kind] {
			return fmt.Errorf("unknown_opcode: %s", instruction.Kind)
		}
		if seen[instruction.ID] {
			return fmt.Errorf("duplicate_instruction: %s", instruction.ID)
		}
		seen[instruction.ID] = true
		if instruction.Index != index {
			return fmt.Errorf("invalid_instruction_index: %s", instruction.ID)
		}
		depth := 1
		for _, reference := range instruction.References {
			if !seen[reference.Step] {
				return fmt.Errorf("cycle_or_future_reference: %s -> %s", instruction.ID, reference.Step)
			}
			if candidate := depths[reference.Step] + 1; candidate > depth {
				depth = candidate
			}
		}
		if depth > maxDepth {
			return fmt.Errorf("program_limits: dependency depth exceeds %d", maxDepth)
		}
		depths[instruction.ID] = depth
	}
	return nil
}

func digestWire(value wire) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func cloneInstructions(input []contract.InternalInstruction) []contract.InternalInstruction {
	output := make([]contract.InternalInstruction, len(input))
	for index, instruction := range input {
		output[index] = instruction
		output[index].References = append([]contract.Reference(nil), instruction.References...)
		for refIndex := range output[index].References {
			output[index].References[refIndex].Path = append([]string(nil), instruction.References[refIndex].Path...)
		}
		if instruction.Source != nil {
			source := *instruction.Source
			output[index].Source = &source
		}
	}
	return output
}
