package devtool

import "github.com/well-prado/new-blok/internal/tooling/layout"

// Code is one stable diagnostic code and the commands that emit it.
type Code struct {
	Code     string
	Commands []string
}

// Codes is the registry of every diagnostic code check, test and inspect
// emit, including layout's, which they pass through unchanged. A code keeps one meaning for the life of blok-cli/v1 (ADR 0024);
// each is a #28 diagnostic code. Tests keep this list, the code literals in
// this package's source and the ADR 0024 table identical.
var Codes = []Code{
	// internal/tooling/layout discovery (ADR 0023), reported by every
	// command unchanged: one code per condition, wherever it is found.
	{layout.CodeManifestMissing, all},
	{layout.CodeManifestInvalid, all},
	{layout.CodeModuleMissing, all},
	{layout.CodePathOutsideRoot, all},
	{layout.CodeMixedLayout, all},
	{layout.CodeInvalidRuntime, all},
	{layout.CodeFileUnowned, all},
	{layout.CodeFileUnsupported, all},
	{layout.CodeParseFailed, all},
	{layout.CodeDescriptorMissing, all},
	{layout.CodeDescriptorMultiple, all},
	{layout.CodeDescriptorNotStatic, all},
	{layout.CodeDescriptorInvalid, all},
	{layout.CodeDescriptorMisplaced, all},
	{layout.CodeDescriptorConstrained, all},
	{layout.CodePackageMismatch, all},
	{layout.CodeRuntimeMismatch, all},
	{layout.CodeDuplicateIdentity, all},
	{layout.CodeDuplicateVersion, all},
	{layout.CodePathCollision, all},
	{layout.CodeOwnershipOverlap, all},
	{layout.CodeWorkflowPathMissing, all},
	{layout.CodeNodeImportsNode, all},
	{layout.CodeNodeImportsWorkflow, all},
	{layout.CodeSymlinkEscape, all},
	{layout.CodeSymlinkAlias, all},
	{layout.CodeSymlinkDangling, all},
	{layout.CodeSymlinkLoop, all},
	{layout.CodeLimitExceeded, all},
	// blok check, test and inspect (ADR 0024).
	{"project_unreadable", all},
	{"bindings_types_missing", checkOnly},
	{"bindings_generate_failed", checkOnly},
	{"bindings_missing", checkOnly},
	{"bindings_not_generated", checkOnly},
	{"bindings_stale", checkOnly},
	{"workflow_step_id_invalid", checkOnly},
	{"workflow_step_id_reserved", checkOnly},
	{"workflow_step_id_duplicate", checkOnly},
	{"go_compile_error", checkAndTest},
	{"go_vet_finding", checkOnly},
	{"go_module_not_in_cache", checkAndTest},
	{"go_module_missing", checkAndTest},
	{"go_mod_needs_update", checkAndTest},
	{"go_sum_missing", checkAndTest},
	{"go_toolchain_too_old", checkAndTest},
	{"go_toolchain_error", checkAndTest},
	{"go_toolchain_unavailable", checkAndTest},
	{"process_guard_unavailable", checkAndTest},
	{"test_failed", testOnly},
	{"test_package_failed", testOnly},
	{"no_tests_ran", testOnly},
	{"interrupted", all},
}

var (
	all          = []string{"check", "test", "inspect"}
	checkOnly    = []string{"check"}
	testOnly     = []string{"test"}
	checkAndTest = []string{"check", "test"}
)

// knownCode reports whether code is in the registry.
func knownCode(code string) bool {
	for _, item := range Codes {
		if item.Code == code {
			return true
		}
	}
	return false
}
