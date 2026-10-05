package devtool

import "github.com/well-prado/new-blok/internal/tooling/layout"

// Code is one stable diagnostic code and the commands that emit it.
type Code struct {
	Code     string
	Commands []string
}

// Codes is the registry of every diagnostic code check, test, inspect and
// dev emit, including layout's, which they pass through unchanged. A code
// keeps one meaning for the life of blok-cli/v1 (ADR 0024) and
// blok-dev/v1 (ADR 0026); each is a #28 diagnostic code. Tests keep this
// list, the code literals in this package's source and the code tables of
// ADRs 0024 and 0026 identical.
var Codes = []Code{
	// internal/tooling/layout discovery (ADR 0023), reported by every
	// command unchanged: one code per condition, wherever it is found.
	{layout.CodeManifestMissing, allAndDev},
	{layout.CodeManifestInvalid, allAndDev},
	{layout.CodeModuleMissing, allAndDev},
	{layout.CodePathOutsideRoot, allAndDev},
	{layout.CodeMixedLayout, allAndDev},
	{layout.CodeInvalidRuntime, allAndDev},
	{layout.CodeFileUnowned, allAndDev},
	{layout.CodeFileUnsupported, allAndDev},
	{layout.CodeParseFailed, allAndDev},
	{layout.CodeDescriptorMissing, allAndDev},
	{layout.CodeDescriptorMultiple, allAndDev},
	{layout.CodeDescriptorNotStatic, allAndDev},
	{layout.CodeDescriptorInvalid, allAndDev},
	{layout.CodeDescriptorMisplaced, allAndDev},
	{layout.CodeDescriptorConstrained, allAndDev},
	{layout.CodePackageMismatch, allAndDev},
	{layout.CodeRuntimeMismatch, allAndDev},
	{layout.CodeDuplicateIdentity, allAndDev},
	{layout.CodeDuplicateVersion, allAndDev},
	{layout.CodePathCollision, allAndDev},
	{layout.CodeOwnershipOverlap, allAndDev},
	{layout.CodeWorkflowPathMissing, allAndDev},
	{layout.CodeNodeImportsNode, allAndDev},
	{layout.CodeNodeImportsWorkflow, allAndDev},
	{layout.CodeSymlinkEscape, allAndDev},
	{layout.CodeSymlinkAlias, allAndDev},
	{layout.CodeSymlinkDangling, allAndDev},
	{layout.CodeSymlinkLoop, allAndDev},
	{layout.CodeLimitExceeded, allAndDev},
	// blok check, test and inspect (ADR 0024).
	{"project_unreadable", allAndDev},
	{"bindings_types_missing", checkAndDev},
	{"bindings_generate_failed", checkAndDev},
	{"bindings_missing", checkAndDev},
	{"bindings_not_generated", checkAndDev},
	{"bindings_stale", checkOnly},
	{"workflow_step_id_invalid", checkOnly},
	{"workflow_step_id_reserved", checkOnly},
	{"workflow_step_id_duplicate", checkOnly},
	{"go_compile_error", buildCommands},
	{"go_vet_finding", checkOnly},
	{"go_module_not_in_cache", buildCommands},
	{"go_module_missing", buildCommands},
	{"go_mod_needs_update", buildCommands},
	{"go_sum_missing", buildCommands},
	{"go_toolchain_too_old", buildCommands},
	{"go_toolchain_error", buildCommands},
	{"go_toolchain_unavailable", buildCommands},
	{"process_guard_unavailable", buildCommands},
	{"test_failed", testOnly},
	{"test_package_failed", testOnly},
	{"no_tests_ran", testOnly},
	{"interrupted", all},
	// blok dev (ADR 0026).
	{"dev_main_package_missing", devOnly},
	{"dev_symlink_unwatched", devOnly},
	{"dev_build_dir_unavailable", devOnly},
	{"dev_bindings_write_failed", devOnly},
	{"dev_app_start_failed", devOnly},
	{"dev_app_exited", devOnly},
	{"dev_app_stop_timeout", devOnly},
}

var (
	all       = []string{"check", "test", "inspect"}
	checkOnly = []string{"check"}
	testOnly  = []string{"test"}
	// blok dev raises layout's codes too, and the bindings and go codes
	// when a build fails (ADR 0026).
	allAndDev     = []string{"check", "test", "inspect", "dev"}
	checkAndDev   = []string{"check", "dev"}
	buildCommands = []string{"check", "test", "dev"}
	devOnly       = []string{"dev"}
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
