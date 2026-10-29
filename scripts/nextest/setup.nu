#!/usr/bin/env nu

# Build the test supervisor, the test runner, and the Scylla client, and export their directory and the cloud databases' environment to the tests that the runner wraps.
def main [] {
	cargo build --quiet --package tangram_scylla_client --package tangram_tests_runner --package tangram_tests_supervisor
	let target_path = cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let debug_path = $target_path | path join debug
	let runner_path = $debug_path | path join tangram_tests_runner
	$env.PATH = $env.PATH | prepend $debug_path
	let path = $env.PATH | str join (char esep)
	let cloud = ^$runner_path database env | lines
	[$"PATH=($path)" ...$cloud]
	| each { $in + "\n" }
	| str join
	| save --append $env.NEXTEST_ENV
}
