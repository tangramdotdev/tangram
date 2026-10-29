#!/usr/bin/env nu

# Build the test supervisor, and export its path to the tests that run under it.
def main [] {
	cargo build --quiet --package tangram_tests_supervisor
	let target_path = cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let path = $target_path | path join debug tangram_tests_supervisor
	$"TANGRAM_TESTS_SUPERVISOR=($path)\n" | save --append $env.NEXTEST_ENV
}
