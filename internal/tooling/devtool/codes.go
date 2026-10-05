package devtool

// Code is one stable diagnostic code and the commands that emit it.
type Code struct {
	Code     string
	Commands []string
}

// Codes is the registry of every diagnostic code check, test and inspect
// emit. A code keeps one meaning for the life of blok-cli/v1 (ADR 0024);
// each is a #28 diagnostic code. Tests keep this list, the code literals in
// this package's source and the ADR 0024 table identical.
var Codes = []Code{
	{"project_go_mod_missing", all},
	{"project_go_mod_invalid", all},
	{"project_manifest_missing", all},
	{"project_manifest_invalid", all},
	{"project_module_mismatch", all},
	{"project_layout_unsupported", all},
	{"project_too_large", all},
	{"project_unreadable", all},
	{"node_import_forbidden", checkOnly},
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
	{"test_failed", testOnly},
	{"test_package_failed", testOnly},
	{"no_tests_ran", testOnly},
	{"source_unreadable", inspectOnly},
	{"source_parse_error", inspectOnly},
	{"interrupted", all},
}

var (
	all          = []string{"check", "test", "inspect"}
	checkOnly    = []string{"check"}
	testOnly     = []string{"test"}
	inspectOnly  = []string{"inspect"}
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
