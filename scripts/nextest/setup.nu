#!/usr/bin/env nu

# Build the test supervisor and the test runner, and export their paths to the tests that run under them.
def main [] {
	cargo build --quiet --package tangram_tests_runner --package tangram_tests_supervisor
	let target_path = cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let debug_path = $target_path | path join debug
	let runner_path = $debug_path | path join tangram_tests_runner
	let supervisor_path = $debug_path | path join tangram_tests_supervisor
	$"TANGRAM_TESTS_RUNNER=($runner_path)\nTANGRAM_TESTS_SUPERVISOR=($supervisor_path)\n" | save --append $env.NEXTEST_ENV
}
