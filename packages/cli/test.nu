#!/usr/bin/env nu

use std/util 'path add'
use ../../scripts/macos/identifiers.nu

const repository_path = path self '../../'

# Build Tangram and the test runner, then run the CLI tests with the runner in packages/cli/packages/test.
def main [
	--accept (-a) # Accept all new and updated snapshots.
	--clean # Clean up leftover test resources from FoundationDB, PostgreSQL, and ScyllaDB.
	--jobs (-j): int # The number of concurrent tests to run.
	--kernel-path: path # The path to the linux kernel image to use with --vm. Required when --vm is set.
	--no-cloud # Use local backends for test instances.
	--preserve-failing-temps # Keep the temporary directories for failed tests.
	--preserve-temps # Keep the temporary directories.
	--no-capture # Do not capture the output of each test. This sets --jobs to 1.
	--no-progress-details # Show only the aggregate progress bar, without listing running tests.
	--offline # Skip tests which require network access.
	--print-passing-test-output # Print the output of passing tests.
	--quickjs # Use QuickJS as the JavaScript engine.
	--release # Use a release build of tangram. Some bugs are only observable in release mode.
	--review (-r) # Review snapshots.
	--stress # Run the matching tests repeatedly until one fails.
	--stress-count: int # Run the matching tests this many times, then stop. Implies --stress.
	--tangram-path: path # Path to a prebuilt tangram binary to use instead of cargo build.
	--timeout: duration = 2min # The timeout for each test.
	--turso # Use Turso for the server database.
	--vfs # Run every test against the VFS: FSKit on macOS and FUSE on Linux. On macOS, this requires the app and its file system extension to be installed and enabled.
	--vm # Use vm isolation as the default for the test harness.
	...filters: string # Filter tests.
] {
	# Validate the flags that affect the build.
	let cloud = $nu.os-info.name in ['linux', 'macos'] and not $no_cloud
	if $release and $tangram_path != null {
		error make { msg: '--release may not be combined with --tangram-path' }
	}
	if $tangram_path != null and not ($tangram_path | path exists) {
		error make { msg: $'--tangram-path does not exist: ($tangram_path)' }
	}
	if $kernel_path != null and not $vm {
		error make { msg: '--kernel-path may only be set with --vm' }
	}
	if $kernel_path != null and not ($kernel_path | path exists) {
		error make { msg: $'--kernel-path does not exist: ($kernel_path)' }
	}
	let fskit = $vfs and $nu.os-info.name == 'macos'

	# Determine which language clients the selected tests require.
	let filter = if ($filters | is-empty) {
		'.*'
	} else {
		$filters | each { '(' + $in + ')' } | str join '|'
	}
	let tests_path = $repository_path | path join 'packages/cli/tests'
	let names = if $clean { [] } else {
		fd -e nu -p $filter $tests_path | lines | each { |path| $path | path relative-to $tests_path }
	}
	let node = $names | any { |name| $name | str starts-with 'node/' }
	let python = $names | any { |name| $name | str starts-with 'python/' }

	# Build and install the current macOS app and file system extension. Isolate its default-feature Cargo build from the all-features test binary.
	if $fskit and not $clean {
		force_unmount_vfs_macos (fskit_temp_root)
		stop_fskit_provider $release
		let build_args = if $release { ['--release'] } else { [] }
		let cargo_target_dir = ($repository_path | path join 'target/macos')
		^bun run macos:build --cargo-target-dir $cargo_target_dir ...$build_args
		^nu ($repository_path | path join 'scripts/macos/install.nu') ...$build_args --no-build
	}

	# Build the test runner with Tangram in one Cargo invocation, and add both to the path. With --tangram-path, use the prebuilt binary instead.
	let profile = if $release { 'release' } else { 'debug' }
	let profile_args = if $release { ['--release'] } else { [] }
	let packages = ['--package' 'tangram_cli_test']
	let packages = if $cloud or $clean { $packages | append ['--package' 'tangram_scylla_client'] } else { $packages }
	let packages = if $node { $packages | append ['--package' 'tangram_javascript_native'] } else { $packages }
	let target_path = $env.CARGO_TARGET_DIR? | default ($repository_path | path join 'target') | path join $profile
	if $tangram_path != null {
		cargo build ...$profile_args ...$packages
		let tangram_path = $tangram_path | path expand
		let bin_path = $repository_path | path join 'target/test-bin'
		mkdir $bin_path
		ln -sf $tangram_path ($bin_path | path join 'tangram')
		ln -sf $tangram_path ($bin_path | path join 'tg')
		path add $target_path
		path add $bin_path
	} else {
		cargo build ...$profile_args --all-features --package tangram_cli ...$packages
		ln -sf tangram ($target_path | path join 'tg')
		path add $target_path
	}

	# Clean up the leftover test resources.
	if $clean {
		let release_args = if $release { ['--release'] } else { [] }
		exec tangram_cli_test clean ...$release_args
	}

	# Build the Node.js client.
	if $node {
		let client_profile = if $release { 'release' } else { 'dev' }
		let client_build_args = if $tangram_path == null { ['--skip-extension-build'] } else { [] }
		bun run --filter @tangramdotdev/client build --profile $client_profile ...$client_build_args
	}

	# Build the Python client with an installed Python 3.12 or later.
	if $python {
		let client_profile = if $release { 'release' } else { 'dev' }
		let python_executable = $env.TANGRAM_PYTHON? | default 'python3'
		^$python_executable packages/clients/python/build.py --profile $client_profile
		path add ($repository_path | path join '.venv/bin')
	}

	# Run the tests.
	let args = [
		(if $accept { ['--accept'] } else { [] })
		(if $cloud { ['--cloud'] } else { [] })
		(if $fskit { ['--fskit'] } else { [] })
		(if $jobs != null { ['--jobs' ($jobs | into string)] } else { [] })
		(if $kernel_path != null { ['--kernel-path' ($kernel_path | path expand)] } else { [] })
		(if $no_capture { ['--no-capture'] } else { [] })
		(if $no_progress_details { ['--no-progress-details'] } else { [] })
		(if $offline { ['--offline'] } else { [] })
		(if $preserve_failing_temps { ['--preserve-failing-temps'] } else { [] })
		(if $preserve_temps { ['--preserve-temps'] } else { [] })
		(if $print_passing_test_output { ['--print-passing-test-output'] } else { [] })
		(if $quickjs { ['--quickjs'] } else { [] })
		(if $review { ['--review'] } else { [] })
		(if $stress { ['--stress'] } else { [] })
		(if $stress_count != null { ['--stress-count' ($stress_count | into string)] } else { [] })
		['--timeout' ($timeout / 1sec | math ceil | into string)]
		(if $turso { ['--turso'] } else { [] })
		(if $vfs { ['--vfs'] } else { [] })
		(if $vm { ['--vm'] } else { [] })
		['--']
		$filters
	] | flatten
	exec tangram_cli_test run ...$args
}

def fskit_provider_pids [executable: path] {
	ps --long
	| where { |process|
		($process.command? | default '') | str starts-with $executable
	}
	| get pid
}

def fskit_temp_root [] {
	$env.HOME | path join '.tangram/test-tmp'
}

# Unmounts any fskit vfs left under the path. A server that exits cleanly unmounts itself, so this only catches the ones that crashed.
def force_unmount_vfs_macos [path: string] {
	let targets = (
		try {
				^timeout 5 mount | lines | each { |line|
				let matches = ($line | parse --regex '^.+ on (?<target>.+) \(tangram[,)]')
				if ($matches | is-empty) { null } else { $matches | first | get target }
			} | compact
		} catch {
			[]
		}
	)
	let store_paths = (
		$targets
		| where { |target| ($target == ($path | path join 'store')) or ($target | str starts-with ($path + '/')) }
		| uniq
		| each { |path| { path: $path, length: ($path | str length) } }
		| sort-by length --reverse
		| get path
	)
	for store_path in $store_paths {
		let output = (^timeout --kill-after 2s 5 umount -f $store_path | complete)
		if $output.exit_code != 0 {
			error make { msg: $'failed to unmount the test VFS: ($store_path)', help: ($output.stderr | str trim) }
		}
	}
}

def stop_fskit_provider [release: bool] {
	let app_name = if $release { 'Tangram' } else { 'Tangram Dev' }
	let executable = (
		$env.HOME
		| path join $'Applications/($app_name).app/Contents/Extensions/TangramFSKit.appex/Contents/MacOS/TangramFSKit'
	)
	let pids = fskit_provider_pids $executable
	for pid in $pids {
		try { kill --quiet $pid }
	}
	for _ in 1..100 {
		if (fskit_provider_pids $executable | is-empty) {
			return
		}
		sleep 50ms
	}
	for pid in (fskit_provider_pids $executable) {
		try { kill --force --quiet $pid }
	}
}
