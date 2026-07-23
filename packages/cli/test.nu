#!/usr/bin/env nu

use std/util 'path add'
use ../../scripts/macos/identifiers.nu

export use std/assert

const repository_path = path self '../../'
const harness_path = path self
const server_exit_directory_name = 'server_jobs'

def main [
	--accept (-a) # Accept all new and updated snapshots.
	--clean # Clean up leftover test resources from cockroach, scylla, and nats.
	--cloud # Enable cloud database backends (cockroach, scylla, nats) for spawn --cloud.
	--fskit # Run every test against the fskit VFS. Requires the macOS app and its file system extension to be installed and enabled.
	--jobs (-j): int # The number of concurrent tests to run.
	--kernel-path: path # The path to the linux kernel image to use with --vm. Required when --vm is set.
	--preserve-temps # Keep the temporary directories.
	--no-capture # Do not capture the output of each test. This sets --jobs to 1.
	--offline # Skip tests which require network access.
	--print-passing-test-output # Print the output of passing tests.
	--quickjs # Use QuickJS as the JS engine.
	--release # Use a release build of tangram. Some bugs are only observable in release mode.
	--review (-r) # Review snapshots.
	--stress # Run the matching tests repeatedly until one fails.
	--stress-count: int # Run the matching tests this many times, then stop. Implies --stress.
	--tangram-path: path # Path to a prebuilt tangram binary to use instead of cargo build.
	--timeout: duration = 60sec # The timeout for each test.
	--turso # Use Turso for the server database.
	--vm # Use vm isolation as the default for the test harness.
	...filters: string # Filter tests.
] {
	# Validate the --vm/--kernel-path flag combination.
	if $vm and $kernel_path == null {
		error make { msg: '--kernel-path is required when --vm is set' }
	}
	if $kernel_path != null and not $vm {
		error make { msg: '--kernel-path may only be set with --vm' }
	}
	if $kernel_path != null and not ($kernel_path | path exists) {
		error make { msg: $'--kernel-path does not exist: ($kernel_path)' }
	}
	# Validate the release flag combination.
	if $release and $tangram_path != null {
		error make { msg: '--release may not be combined with --tangram-path' }
	}
	# Validate the fskit flag.
	if $fskit and $nu.os-info.name != 'macos' {
		error make { msg: '--fskit is only supported on macos' }
	}
	# Validate the stress flag combination.
	let stress = $stress or $stress_count != null
	if $stress and ($accept or $review) {
		error make { msg: '--stress may not be combined with --accept or --review' }
	}
	if $stress_count != null and $stress_count < 1 {
		error make { msg: '--stress-count must be at least one' }
	}
	# Clean up leftover test resources if requested.
	if $clean {
		let fskit_temp_paths = if (fskit_temp_root | path exists) {
			ls (fskit_temp_root) | where name =~ 'tangram_test_' and type == dir | get name
		} else {
			[]
		}
		let test_temp_paths = (
			ls ($nu.temp-dir? | default $nu.temp-path?)
			| where name =~ 'tangram_test_' and type == dir
			| get name
			| append $fskit_temp_paths
		)
		let lmdb_sysv_keys = lmdb_sysv_keys_for_test_dirs $test_temp_paths

		for path in $test_temp_paths {
			remove_temp_directory $path
			print -e $"removed ($path)"
		}

		let cockroach_dbs = cockroach sql --insecure --host=localhost:26257 --format=csv -e 'show databases;' | from csv | get database_name | where { $in starts-with 'database_' }
		for db in $cockroach_dbs {
			print -e $"dropping cockroach database ($db)"
			try { cockroach sql --insecure --host=localhost:26257 -e $'drop database if exists ($db) cascade' }
		}

		let preserved_keyspaces = ['system', 'system_auth', 'system_distributed', 'system_distributed_everywhere', 'system_schema', 'system_traces', 'system_views', 'objects']
		let keyspaces = cqlsh -e "SELECT JSON keyspace_name FROM system_schema.keyspaces" | lines | str trim | where { $in starts-with '{' } | each { $in | from json | get keyspace_name } | where { $in starts-with 'objects_' }
		for keyspace in $keyspaces {
			print -e $"dropping scylla keyspace ($keyspace)"
			try { cqlsh -e $"drop keyspace \"($keyspace)\";" e> /dev/null }
		}

		print -e "clearing fdb data"
		let cluster = fdb_cluster
		try { ^timeout 10 fdbcli -C $cluster --exec 'writemode on; clearrange "" "\xff"' }

		let tangram_processes = count_tangram_processes
		if $tangram_processes > 0 {
			clean_tangram_processes
		}
		let remaining_tangram_processes = count_tangram_processes
		print -e $"cleaned tangram processes: ($tangram_processes - $remaining_tangram_processes)"

		let lmdb_sysv_semaphores = count_lmdb_sysv_semaphores $lmdb_sysv_keys
		if $lmdb_sysv_semaphores > 0 {
			clean_lmdb_sysv_semaphores $lmdb_sysv_keys
		}
		let remaining_lmdb_sysv_semaphores = count_lmdb_sysv_semaphores $lmdb_sysv_keys
		print -e $"cleaned lmdb sysv semaphores: ($lmdb_sysv_semaphores - $remaining_lmdb_sysv_semaphores)"

		return
	}

	# Build and install the current macOS app and file system extension. Isolate
	# its default-feature Cargo build from the all-features test binary.
	if $fskit {
		let build_args = if $release { ['--release'] } else { [] }
		let cargo_target_dir = ($repository_path | path join 'target/macos')
		^bun run macos:build --cargo-target-dir $cargo_target_dir ...$build_args
		^nu ($repository_path | path join 'scripts/macos/install.nu') ...$build_args --no-build
	}

	# Add the tangram binary to the path. If --tangram-path was provided, use
	# its parent directory directly and place the tg symlink in a temp dir;
	# otherwise build from source.
	if $tangram_path != null {
		if not ($tangram_path | path exists) {
			error make { msg: $'--tangram-path does not exist: ($tangram_path)' }
		}
		let tangram_path = $tangram_path | path expand
		let tg_dir = mktemp -d -t tangram_test_tg_XXXXXX
		ln -sf $tangram_path ($tg_dir | path join 'tg')
		path add ($tangram_path | path dirname)
		path add $tg_dir
	} else if $release {
		cargo build --release --all-features
		ln -sf tangram target/release/tg
		path add ($repository_path | path join 'target/release')
	} else {
		cargo build --all-features
		ln -sf tangram target/debug/tg
		path add ($repository_path | path join 'target/debug')
	}

	# Get the matching tests.
	let filter = if ($filters | is-empty) {
		'.*'
	} else {
		$filters | each { '(' + $in + ')' } | str join '|'
	}
	let tests_path = ($repository_path | path join 'packages/cli/tests')
	let tests = fd -e nu -p $filter $tests_path | lines | sort | where { |path|
		not (($path | path relative-to $tests_path) | str starts-with 'lib/')
	} | each { |path|
		{
			path: $path,
			name: ($path | path relative-to $tests_path)
		}
	}

	mut results = []

	# Determine the number of concurrent tests to run.
	let jobs = $jobs | default (sys cpu | length)
	let jobs = if $no_capture {
		1
	} else {
		$jobs
	}

	let options = {
		cloud: $cloud,
		fskit: $fskit,
		offline: $offline,
		quickjs: $quickjs,
		no_capture: $no_capture,
		preserve_temps: $preserve_temps,
		stress: $stress,
		timeout: $timeout,
		turso: $turso,
		vm: $vm,
		kernel_path: ($kernel_path | default "" | into string),
	}
	if $no_capture {
		mut round = 1
		mut stop = false
		while not $stop {
			for test in $tests {
				let result = run_test $test $options
				print_test_result $result $print_passing_test_output
				$results = $results | append $result
				if $stress and (is_failed $result) {
					print -e $'(ansi red)($result.name) failed on round ($round)(ansi reset)'
					$stop = true
					break
				}
			}
			if not $stress or ($stress_count != null and $round >= $stress_count) {
				$stop = true
			}
			$round = $round + 1
		}
	} else {
		# Create the state.
		mut pending = round_entries $tests 1 0
		mut next_seq = $tests | length
		mut round = 1
		mut stress_stopped = false
		mut running = []

		let start = date now
		let total = if $stress {
			if $stress_count != null { ($tests | length) * $stress_count } else { 0 }
		} else {
			$pending | length
		}
		let total_display = if $stress and $stress_count == null { '∞' } else { $total }

		def spawn [test: record] {
			job spawn {
				let start = date now
				let result = try {
					run_test $test $options
				} catch { |error|
					{
						duration: ((date now) - $start),
						name: $test.name,
						output: {
							exit_code: 1,
							stdout: '',
							stderr: ($error | get msg | default ($error | to nuon)),
						},
						temp_path: '',
					}
				}
				$result | merge { seq: $test.seq, round: $test.round } | job send 0
			}
		}

		# Spawn a job that sends a null message every 100ms to trigger progress updates.
		let interval_job = job spawn {
			loop {
				sleep 1sec
				null | job send 0
			}
		}

		# Hide the cursor.
		print -e -n "\e[?25l"

		# Process results as they complete.
		while ($running | length) > 0 or ($pending | length) > 0 {
			# Keep the worker pool full. In stress mode, refill the queue with the next round as necessary, so the pool stays full even when fewer tests match than there are jobs.
			while ($running | length) < $jobs {
				if ($pending | is-empty) {
					if $stress and not $stress_stopped and ($stress_count == null or $round < $stress_count) {
						$round = $round + 1
						$pending = round_entries $tests $round $next_seq
						$next_seq = $next_seq + ($tests | length)
					} else {
						break
					}
				}
				let test = $pending | first
				$pending = $pending | skip 1
				let id = spawn $test
				$running = $running | append { id: $id, seq: $test.seq, name: $test.name, start: (date now) }
			}

			# Wait for the next event (either test completion or ticker).
			let result = job recv

			# Clear the from the cursor to the end.
			print -e -n "\e[0J"

			if $result != null {
				print_test_result $result $print_passing_test_output

				# Store the result.
				$results = $results | append $result

				# Remove the completed job from the running list.
				$running = $running | where seq != $result.seq

				# In stress mode, stop spawning new tests after the first failure.
				if $stress and (is_failed $result) {
					print -e $'(ansi red)($result.name) failed on round ($result.round)(ansi reset)'
					$pending = []
					$stress_stopped = true
				}
			}

			# Print the running tests.
			let term_width = term size | get columns
			for test in $running {
				let duration = ((date now) - $test.start) / 1sec | math floor | into duration -u sec
				let text = $'($test.name) ($duration)'
				let max_length = $term_width - 2
				let text = if ($text | str length) > $max_length {
					($text | str substring ..($max_length - 2)) + '…'
				} else {
					$text
				}
				print -e $'(ansi blue)●(ansi reset) ($text)'
			}

			# Print the progress bar.
			let completed = $results | length
			let passed = $results | where output.exit_code == 0 | length
			let skipped = $results | where output.exit_code == 77 | length
			let failed = $results | where { |result| is_failed $result } | length
			let ratio = if $total > 0 { $completed / $total } else { 0 }
			let filled = ($ratio * 10) | math floor
			let bar = if $filled > 0 { (1..$filled | each { '=' } | str join) + '>' } else { '>' }
			let bar = if $filled < 10 { $bar + (1..(9 - $filled) | each { ' ' } | str join) } else { $bar }
			let elapsed = ((date now) - $start) / 1sec | math floor | into duration -u sec
			let progress = $'[($bar)] ($completed)/($total_display): ($running | length) running, (ansi green)($passed) passed(ansi reset), (ansi yellow)($skipped) skipped(ansi reset), (ansi red)($failed) failed(ansi reset), ($elapsed)'
			print -e -n $'($progress)'

			# Move the cursor up.
			print -e -n $"\r"
			if ($running | length) > 0 {
				print -e -n $"\e[($running | length)A"
			}
		}

		job kill $interval_job

		# Clear.
		print -e -n "\e[0J"

		# Show the cursor.
		print -e -n "\e[?25h"
	}

	if $accept {
		for test in $tests {
			let parsed = $test.path | path parse

			# Accept all pending file snapshots.
			for pending_path in (glob $'($parsed.parent | path join $parsed.stem){.{pending},/*.{pending}}') {
				let snapshot_path = $pending_path | str replace '.pending' '.snapshot'
				mv -f $pending_path $snapshot_path
			}

			# Accept all inline snapshots.
			let inline_paths = glob $'($parsed.parent | path join $parsed.stem).inline'
			for inline_path in $inline_paths {
				let entries = open $inline_path | from json
				let sorted_entries = $entries | sort-by position --reverse
				mut source = open $test.path
				for entry in $sorted_entries {
					let before = $source | str substring ..<$entry.position
					let indent = get_indent $source $entry.position
					let after = $source | str substring ($entry.position + $entry.length)..
					$source = $before ++ (literal $entry.new $indent) ++ $after
				}
				$source | save -f $test.path
				rm $inline_path
			}
		}
	}

	if $review {
		for test in $tests {
			let parsed = $test.path | path parse

			let pending_paths = glob $'($parsed.parent | path join $parsed.stem){.{pending},/*.{pending}}'
			for pending_path in $pending_paths {
				let snapshot_path = $pending_path | str replace '.pending' '.snapshot'
				clear -k
				if ($snapshot_path | path exists) {
					print -e $'(ansi yellow)changed(ansi reset) ($snapshot_path)'
					diff $snapshot_path $pending_path --path | print -e
				} else {
					print -e $'(ansi green)added(ansi reset) ($snapshot_path)'
					print -e ''
					print -e -n (ansi green)
					open $pending_path | print -e
					print -e (ansi reset)
				}
				print -e ''
				print -e -n $'(ansi green)[a]ccept(ansi reset) or (ansi red)[r]eject(ansi reset): '
				loop {
					let response = input -n 1 -s
					if $response == 'a' {
						mv -f $pending_path $snapshot_path
						break
					} else if $response == 'r' {
						rm $pending_path
						break
					}
				}
				print -e ''
			}

			let inline_paths = glob $'($parsed.parent | path join $parsed.stem).inline'
			for inline_path in $inline_paths {
				let entries = open $inline_path | from json
				for entry in $entries {
					clear -k
					print -e $'(ansi yellow)changed(ansi reset) ($test.path)'
					diff $entry.old $entry.new | print -e
					print -e ''
				}
				print -e -n $'(ansi green)[a]ccept(ansi reset) or (ansi red)[r]eject(ansi reset): '
				loop {
					let response = input -n 1 -s
					if $response == 'a' {
						let sorted_entries = $entries | sort-by position --reverse
						mut source = open $test.path
						for entry in $sorted_entries {
							let before = $source | str substring ..<$entry.position
							let indent = get_indent $source $entry.position
							let after = $source | str substring ($entry.position + $entry.length)..
							$source = $before ++ (literal $entry.new $indent) ++ $after
						}
						$source | save -f $test.path
					} else if $response != 'r' {
						continue
					}
					rm $inline_path
					break
				}
				print -e ''
			}

			# Delete snapshots which were not touched and remove touched files.
			if ($pending_paths | length) > 0 or ($inline_paths | length) > 0 {
				for path in (glob $'($parsed.parent | path join $parsed.stem){.snapshot,/*.snapshot}') {
					if not ($path | str replace '.snapshot' '.touched' | path exists) {
						rm $path
					}
				}
				for path in (glob $'($parsed.parent | path join $parsed.stem){.touched,/*.touched}') {
					rm $path
				}
			}
		}
	}

	# Print the summary.
	let passed = $results | where output.exit_code == 0 | length
	let skipped = $results | where output.exit_code == 77 | length
	let failed = $results | where { |result| is_failed $result }
	let total = $results | length
	print -e $'(ansi green)($passed) passed(ansi reset), (ansi yellow)($skipped) skipped(ansi reset), (ansi red)($failed | length) failed(ansi reset), ($total) total'

	# Print the failed tests.
	for result in $failed {
		print -e $'(ansi red)✗(ansi reset) ($result.name) ($result.duration)'
	}

	if $preserve_temps {
		print -e ''
		print -e 'preserved temp directories:'
		for result in $results {
			print -e $'  ($result.name): ($result.temp_path)'
		}
	}

	if not ($failed | is-empty) {
		exit 1
	}
}

# Report whether a result represents a failure. Exit code 77 means the test was skipped.
def is_failed [result: record] {
	$result.output.exit_code != 0 and $result.output.exit_code != 77
}

# Create the pending entries for one round of tests. Each entry carries a unique sequence number, because in stress mode the same test may run concurrently with itself, so results cannot be matched to running entries by name.
def fskit_temp_root [] {
	$env.HOME | path join '.tangram/test-tmp'
}

def round_entries [tests: list, round: int, first_seq: int] {
	$tests | enumerate | each { |entry| $entry.item | merge { seq: ($first_seq + $entry.index), round: $round } }
}

def run_test [test: record, options: record] {
	# Create a temp directory for this test. With fskit, it must live under the
	# tangram directory, because that is the only path outside the app group
	# container the file system extension's sandbox permits.
	let temp_path = if $options.fskit {
		let root = fskit_temp_root
		mkdir $root
		mktemp -d --tmpdir-path $root 'tangram_test_XXXXXX' | path expand
	} else {
		mktemp -d -t tangram_test_XXXXXX | path expand
	}

	# Remove inline, pending, and touch files. Skip this in stress mode, because concurrent runs of the same test would race on these files.
	let parsed = $test.path | path parse
	if not $options.stress {
		for path in (glob $'($parsed.parent | path join $parsed.stem){.{inline,pending,touched},/*.{pending,touched}}') {
			rm $path
		}
	}

	# Run the test.
	let start = date now
	let timeout = $options.timeout | into int | $in / 1_000_000_000
	mut config = {}
	if $options.preserve_temps {
		$config = $config | merge deep {
			advanced: {
				preserve_temp_directories: true,
			},
		}
	}
	if $options.vm {
		$config = $config | merge deep {
			sandbox: {
				isolation: {
					vm : {
						kernel_path: $options.kernel_path,
					},
				},
			},
		}
	}
	if not ($config | is-empty) {
		$config | to json | save -f ($temp_path | path join "config.json")
	}
	let output = with-env {
		SHELL: "/bin/sh",
		TANGRAM_CONFIG: ($temp_path | path join "config.json"),
		TANGRAM_MODE: client,
		TANGRAM_QUIET: true,
		TANGRAM_TEST_CLOUD: (if $options.cloud { "1" } else { "" }),
		TANGRAM_TEST_FSKIT: (if $options.fskit { "1" } else { "" }),
		TANGRAM_TEST_OFFLINE: (if $options.offline { "1" } else { "" }),
		TANGRAM_TEST_QUICKJS: (if $options.quickjs { "1" } else { "" }),
		TANGRAM_TEST_TURSO: (if $options.turso { "1" } else { "" }),
		TANGRAM_TEST_VM: (if $options.vm { "1" } else { "" }),
		TANGRAM_TEST_KERNEL_PATH: $options.kernel_path,
		TMPDIR: $temp_path,
	} {
		let command = [
			$'use ($harness_path) cleanup_background_jobs'
			'$env.config.display_errors.exit_code = true;'
			$'source ($test.path);'
			$'cleanup_background_jobs ($temp_path);'
		] | str join "\n"
		if $options.no_capture {
			open /dev/null | timeout --kill-after 5s $timeout bash -c (process_supervisor) _ $nu.pid nu -c $command o+e> /dev/stderr
			let exit_code = $env.LAST_EXIT_CODE
			{ exit_code: $exit_code, stdout: '', stderr: '' }
		} else {
			let output = open /dev/null | timeout --kill-after 5s $timeout bash -c (process_supervisor) _ $nu.pid nu -c $command o+e>| complete
			{
				exit_code: $output.exit_code,
				stdout: '',
				stderr: $output.stdout,
			}
		}
	}
	let end = date now
	let duration = $end - $start

	# If the test passed, delete snapshots which were not touched and remove touch files. Skip this in stress mode, because concurrent runs of the same test would race on these files.
	if $output.exit_code == 0 and not $options.stress {
		let parent_path = $test.path | path dirname
		let stem = $test.path | path parse | get stem
		for path in (glob $'($parent_path | path join $stem){.snapshot,/*.snapshot}') {
			if not ($path | str replace '.snapshot' '.touched' | path exists) {
				rm $path
			}
		}
		for path in (glob $'($parent_path | path join $stem){.touched,/*.touched}') {
			try { rm $path }
		}
	}

	# Clean up the cloud resource.
	let ids_path = $temp_path | path join 'ids'
	let ids = if ($ids_path | path exists) {
		open $ids_path | lines | where { $in != '' }
	} else {
		[]
	}
	for id in $ids {
		clean_databases $id
	}

	cleanup_background_jobs $temp_path

	# Clean up the temp directory.
	if not $options.preserve_temps {
		remove_temp_directory $temp_path
	}

	{
		duration: $duration,
		name: $test.name,
		output: $output,
		temp_path: $temp_path,
	}
}

def print_test_result [result: record, print_passing_test_output: bool] {
	let symbol = if $result.output.exit_code == 0 {
		$'(ansi green)✓(ansi reset)'
	} else if $result.output.exit_code == 77 {
		$'(ansi yellow)⊘(ansi reset)'
	} else {
		$'(ansi red)✗(ansi reset)'
	}
	print -e $'($symbol) ($result.name) ($result.duration)'
	if $result.output.exit_code == 77 {
		let reason = $result.output.stderr | str trim
		if ($reason | str length) > 0 {
			print -e $'  ($reason)'
		}
	} else if $print_passing_test_output or $result.output.exit_code != 0 {
		print -e -n $result.output.stderr
	}
}

export def artifact [artifact] {
	def inner [artifact: any, path: string] {
		let artifact = if ($artifact | describe) == 'string' {
			{ kind: 'file', contents: (doc $artifact), executable: false }
		} else if (($artifact | describe) | str starts-with 'record') {
			if $artifact.kind? != null {
				$artifact
			} else {
				{ kind: 'directory', entries: $artifact }
			}
		} else {
			$artifact
		}
		match $artifact.kind {
			'directory' => {
				try { mkdir $path }
				for entry in ($artifact.entries | transpose name value) {
					inner $entry.value ($path | path join $entry.name)
				}
			}
			'file' => {
				$artifact.contents | save $path
				if $artifact.executable {
					chmod +x $path
				}
				for pair in (($artifact.xattrs? | default {}) | transpose key value) {
					xattr_write $pair.key $pair.value $path
				}
			}
			'symlink' => {
				ln -s $artifact.path $path
			}
		}
	}
	let path = mktemp -d | path join 'artifact'
	inner $artifact $path
	$path
}

export def directory [entries: record] {
	{ kind: 'directory', entries: $entries }
}

export def file [
	--executable (-x)
	--xattrs: record
	contents?: string
] {
	{ kind: 'file', contents: (doc ($contents | default '')), executable: $executable, xattrs: $xattrs }
}

export def symlink [path: string] {
	{ kind: 'symlink', path: $path }
}

export def doc [string: string] {
	# Split the lines.
	mut lines = $string | split row "\n"

	# Remove the first line if it is empty or contains only whitespace.
	if ($lines | length) > 0 and (($lines | first | str trim | str length) == 0) {
		$lines = $lines | skip 1
	}
	if ($lines | length) > 0 {
		let last = $lines | last
		if ($last | str trim | str length) == 0 {
			$lines = $lines | drop
		}
	}

	# Get the common leading whitespace prefix. Filter out lines that are empty or contain only whitespace.
	let non_whitespace_lines = $lines | where { |line|
		let trimmed = $line | str trim
		($trimmed | str length) > 0
	}

	# Extract leading whitespace from each non-empty line.
	let leading_whitespace = $non_whitespace_lines | each { |line|
		$line | split chars | take while { |char| $char == "\t" or $char == " " } | str join
	}

	# Find the common prefix of all leading whitespace strings.
	let common_prefix = if ($leading_whitespace | length) > 0 {
		$leading_whitespace | reduce { |it, acc|
			let acc_len = $acc | str length
			let it_len = $it | str length
			let min_len = if $acc_len < $it_len { $acc_len } else { $it_len }
			mut prefix_len = 0
			let acc_chars = $acc | split chars
			let it_chars = $it | split chars
			for i in 0..<$min_len {
				if ($acc_chars | get $i) == ($it_chars | get $i) {
					$prefix_len = $prefix_len + 1
				} else {
					break
				}
			}
			$acc | str substring ..<$prefix_len
		}
	} else {
		""
	}

	let prefix_len = $common_prefix | str length

	# Remove the common prefix from each line and combine them with newlines.
	let result = $lines
		| each { |line|
			if ($line | str length) >= $prefix_len {
				$line | str substring $prefix_len..
			} else {
				$line
			}
		}
		| str join "\n"

	$result
}

export def --env snapshot [
	--name: string
	--normalize (-n)
	--normalize-ids
	--path (-p)
	--redact (-r): oneof<list<string>, string>
	value: any
	inline?: string
] {
	let value = if $path {
		snapshot_path $value | to json -i 2
	} else {
		$value | to text
	}
	let redactions = if $redact == null {
		null
	} else if ($redact | describe) == 'string' {
		[$redact]
	} else {
		$redact
	}
	let value = if $redactions == null { $value } else { $value | redact ...$redactions }
	let value = if $normalize_ids {
		$value | normalize --normalize-ids
	} else if $normalize {
		$value | normalize
	} else {
		$value
	}

	if $inline != null {
		snapshot_inline --span=(metadata $inline).span $value $inline
	} else {
		snapshot_file --name=$name $value
	}
}

def --env snapshot_inline [
	--span: record
	value: string
	inline: string
] {
	# Get the expected value by processing the snapshot with doc.
	let expected_value = doc $inline

	# If the values match, return early.
	if $value == $expected_value {
		return
	}

	# Save the inline snapshot.
	let test_path = $env.CURRENT_FILE
	let test_name = $test_path | path parse | get stem
	let test_directory_path = $test_path | path dirname
	let inline_path = $test_directory_path | path join $'($test_name).inline'

	# Read existing inline data or start fresh.
	mut inline_entries = if ($inline_path | path exists) {
		open $inline_path | from json
	} else {
		[]
	}

	# Get the exact file position using view files.
	let files = view files
	let file = $files | where { |f| $span.start >= $f.start and $span.start < $f.end } | first
	let position = $span.start - $file.start
	let length = $span.end - $span.start

	# Add this entry.
	$inline_entries = $inline_entries | append {
		position: $position,
		length: $length,
		old: $expected_value,
		new: $value,
	}

	$inline_entries | to json | save -f $inline_path

	error make {
		msg: 'the snapshot does not match',
		help: (diff $expected_value $value),
		label: {
			span: $span,
			text: 'the snapshot',
		},
	}
}

def --env snapshot_file [
	--name (-n): string
	value: string
] {
	# Get the snapshot path.
	let test_path = $env.CURRENT_FILE
	let test_name = $test_path | path parse | get stem
	let test_directory_path = $test_path | path dirname
	let snapshot_directory_path = $test_directory_path | path join $test_name
	if $name != null {
		try { mkdir -v $snapshot_directory_path }
	}
	let snapshot_path = if $name == null {
		$test_directory_path | path join $'($test_name).snapshot'
	} else {
		$snapshot_directory_path | path join $'($name).snapshot'
	}
	let pending_path = $snapshot_path | str replace '.snapshot' '.pending'
	let touched_path = $snapshot_path | str replace '.snapshot' '.touched'

	# Touch the snapshot.
	touch $touched_path

	# Error if the snapshot does not exist.
	if not ($snapshot_path | path exists) {
		$value | save -f $pending_path
		error make {
			msg: 'the snapshot does not exist',
			label: {
				span: (metadata $value).span,
				text: 'the value',
			}
		}
	}

	# Read the snapshot.
	let old_value = open $snapshot_path

	# Error if the new value does not match the old value.
	if $value != $old_value {
		$value | save -f $pending_path
		error make {
			msg: 'the snapshot does not match',
			help: (diff $snapshot_path $pending_path --path),
			label: {
				span: (metadata $value).span,
				text: 'the value',
			},
		}
	}
}

def snapshot_path [path: string] {
	let $type = $path | path type
	if $type == 'dir' {
		let entries = ls -a $path
			| where name != ($path | path join '.') and name != ($path | path join '..')
			| each { |entry|
					let name = $entry.name | path basename
					let artifact = snapshot_path $entry.name
					{ name: $name, artifact: $artifact }
				}
			| reduce -f {} { |entry, acc|
					$acc | insert $entry.name $entry.artifact
				}
		{ kind: 'directory', entries: $entries }
	} else if $type == 'file' {
		let contents = open $path
		let executable = ls -l $path | first | get mode | str contains 'x'
		let names = xattr_list $path | where { |name| $name starts-with 'user.tangram' }
		let xattrs = $names | reduce -f {} { |name, acc| $acc | insert $name (xattr_read $name $path) }
		mut output = { kind: 'file', contents: $contents }
		if $executable {
			$output.executable = true
		}
		if not ($xattrs | is-empty) {
			$output.xattrs = $xattrs
		}
		$output
	} else if $type == 'symlink' {
		mut target = do -i { ls -l $path | first | get target }
		if $target == null {
			$target = (readlink $path | str trim)
		}
		{ kind: 'symlink', path: $target }
	}
}

export def --env spawn [
	--busybox
	--cloud
	--config (-c): record
	--directory (-d): string
	--name (-n): string
	--quickjs # Use QuickJS as the JS engine.
	--url (-u): string
] {
	# Give the object store's lock semaphores a unique prefix per server. lmdb
	# appends 'r' and 'w' to form the two POSIX semaphore names, which are global
	# to the machine, so concurrent test servers must not share a prefix. The
	# prefix plus one character must fit the platform limit, which is 31
	# characters on macOS.
	let object_store_posix_sem_prefix = $'/tg-((random chars) | str lowercase | str substring 0..7)'

	mut default_config = {
		advanced: {
			disable_version_check: true
			internal_error_locations: false
		},
		index: {
			kind: 'lmdb',
			map_size: 10_485_760,
		},
		logs: {
			store: {
				kind: 'lmdb',
				map_size: 10_485_760,
			},
		},
		object: {
			store: {
				kind: 'lmdb',
				map_size: 10_485_760,
				posix_sem_prefix: $object_store_posix_sem_prefix,
			},
		},
		remotes: {},
		tokio_single_threaded: true,
		v8_thread_pool_size: 1,
	}

	let use_quickjs = $quickjs or (($env.TANGRAM_TEST_QUICKJS? | default "") | str length) > 0
	if $use_quickjs {
		$default_config = $default_config | merge deep {
			runner: {
				js: {
					engine: 'quickjs',
				},
			},
		}
	}

	let use_turso = (($env.TANGRAM_TEST_TURSO? | default "") | str length) > 0
	if $use_turso {
		$default_config = $default_config | merge deep {
			database: {
				kind: 'turso',
				path: 'database',
			},
		}
	}

	let use_fskit = (($env.TANGRAM_TEST_FSKIT? | default "") | str length) > 0
	if $use_fskit {
		$default_config = $default_config | merge deep {
			vfs: {
				kind: 'fskit',
			},
		}
	}


	let use_vm = (($env.TANGRAM_TEST_VM? | default "") | str length) > 0
	if $use_vm {
		let kernel_path = $env.TANGRAM_TEST_KERNEL_PATH? | default ""
		if ($kernel_path | str length) == 0 {
			error make { msg: 'TANGRAM_TEST_VM is set but TANGRAM_TEST_KERNEL_PATH is empty' }
		}
		$default_config = $default_config | merge deep {
			sandbox: {
				isolation: {
					vm: {
						kernel_path: $kernel_path,
					},
				},
			},
		}
	}

	mut id: any = null
	let use_cloud = $cloud and (($env.TANGRAM_TEST_CLOUD? | default "") | str length) > 0
	if $use_cloud {
		$id = ((random chars) | str lowercase)
		$id ++ "\n" | save --append (($nu.temp-dir? | default $nu.temp-path?) | path join 'ids')
		print -e $id

		cockroach sql --insecure --host=localhost:26257 -e $'create database database_($id)'
		cockroach sql --insecure --host=localhost:26257 -d $'database_($id)' -f ($repository_path | path join packages/server/src/database/postgres.sql)

		let cluster = fdb_cluster

		cqlsh -e $"create keyspace \"objects_($id)\" with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };"
		cqlsh -k $'objects_($id)' -f ($repository_path | path join packages/stores/object/src/scylla.cql)

		let config = {
			database: {
				kind: 'postgres',
				pool: {
					max: 1,
				},
				url: $'postgres://root@localhost:26257/database_($id)?sslmode=disable',
			},
			index: {
				cluster: $cluster,
				kind: 'fdb',
				prefix: $'index_($id)',
			},
			logs: {
				store: {
					cluster: $cluster,
					kind: 'fdb',
					prefix: $'logs_($id)',
				},
			},
			messenger: {
				id: $id,
				kind: 'nats',
				url: 'nats://localhost',
			},
			object: {
				store: {
					addr: 'localhost:9042',
					connections: 1,
					keyspace: $'objects_($id)',
					kind: 'scylla',
				},
			},
			remotes: {},
			watchdog: {
				interval: 1,
				ttl: 60
			}
		}
		$default_config = $default_config | merge $config
	}

	# Write the config.
	let config = $default_config | merge deep --strategy append ($config | default {})

	# Force the vfs kind for every server when fskit is enabled, because a test that configures the vfs itself would otherwise override it. A test that disables the vfs is left alone.
	let config = if not $use_fskit {
		$config
	} else if ($config | get --optional vfs) == false {
		$config
	} else if (($config | get --optional vfs | describe) | str starts-with 'record') {
		$config | upsert vfs ($config | get vfs | upsert kind 'fskit')
	} else {
		$config | upsert vfs { kind: 'fskit' }
	}
	let config_path = mktemp -d
	let config_path = $config_path | path join 'config.json'
	$config | to json | save -f $config_path

	# Create the directory.
	let directory_path = $directory | default (mktemp -d)

	# Determine the url.
	let url = $url | default $'http+unix://($directory_path | url encode --all)%2Fsocket'
	$env.TANGRAM_URL = $url

	# On macOS, give the server a unique socket for the fskit file system extension
	# to connect to, so concurrent servers and multiple mounts do not collide. The
	# sandboxed extension can only reach the shared app group container, so the
	# socket must live there.
	if $nu.os-info.name == 'macos' {
		let group_id = (identifiers).app_group_identifier
		let group_container = $env.HOME | path join 'Library/Group Containers' $group_id
		try { mkdir $group_container }
		let socket_name = $'socket-((random chars) | str lowercase)'
		$env.TANGRAM_MACOS_APP_GROUP_SOCKET = ($group_container | path join $socket_name)
	}

	# Create a path for server readiness signaling.
	let ready_path = ($config_path | path dirname | path join 'ready')
	touch $ready_path
	let server_exit_directory_path = (($env.TMPDIR? | default ($config_path | path dirname)) | path join $server_exit_directory_name)
	try { mkdir $server_exit_directory_path }

	# Spawn the server.
	let server_job = job spawn -d server {
		let server_job_id = job id
		let exit_path = $server_exit_directory_path | path join $'($server_job_id).exit'
		do -i {
			bash -c $"
				PARENT_PID=$PPID
				SELF_PID=$$
					\(
						while kill -0 $PARENT_PID 2>/dev/null; do
							sleep 0.05
						done
						kill -TERM -$SELF_PID 2>/dev/null || true
					\) &
				exec 3>\"($ready_path)\"
				exec tangram -c \"($config_path)\" -d \"($directory_path)\" -u \"($url)\" serve --ready-fd 3
			" e>| lines | each { |line| print -e $"($name | default 'server'): ($line)\r" }
		}
		'' | save -f $exit_path
	}

	# Wait for the server to be ready.
	let ready_timeout = 30sec
	let ready_timeout_secs = $ready_timeout | into int | $in / 1_000_000_000
	let ready_output = (open /dev/null | timeout $ready_timeout_secs bash -c 'while [ ! -s "$1" ]; do sleep 0.05; done; od -An -t u1 -N1 "$1"' _ $ready_path | complete)
	rm -f $ready_path
	let ready_byte = ($ready_output.stdout | str trim)
	if $ready_output.exit_code != 0 {
		stop_server_job $server_job
		error make {
			msg: $"the server did not signal readiness within ($ready_timeout)"
		}
	}
	if $ready_byte != '0' {
		stop_server_job $server_job
		error make {
			msg: (
				if ($ready_byte | is-empty) {
					'the server exited before signaling readiness; check the server output above'
				} else {
					$"the server signaled an invalid readiness byte: ($ready_byte)"
				}
			)
		}
	}

	# Tag busybox if requested.
	if $busybox {
		skip_if_offline
		let path = mktemp -d
		let source = '
			const SOURCES: Record<string, { url: string, checksum: tg.Checksum }> = {
				"aarch64-darwin": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/utils_universal_darwin.tar.zst",
					checksum: "sha256:8e0031b8c5a183e173fe4b7c2d6b038c46b46f390f6ff5e1d23eb0ec403e2abe",
				},
				"x86_64-darwin": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/utils_universal_darwin.tar.zst",
					checksum: "sha256:8e0031b8c5a183e173fe4b7c2d6b038c46b46f390f6ff5e1d23eb0ec403e2abe",
				},
				"aarch64-linux": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/utils_aarch64_linux.tar.zst",
					checksum: "sha256:b4724cfba44ea545fb041c61cdd86c0c8fdda1f221bfbe284c23853014faec6d",
				},
				"x86_64-linux": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/utils_x86_64_linux.tar.zst",
					checksum: "sha256:552e634483b6d118463bff342febc2b72665c48912e0bf90e80c897cf20b16a9",
				},
			};

			export const env = (host?: string) => {
				const host_ = host ?? tg.host.current;
				tg.assert(typeof host_ === "string");
				const kv = Object.entries(SOURCES).find(([k, _]) => k === host_);
				tg.assert(kv, `unknown host: ${host_}`);
				const { url, checksum } = kv[1];
				const dir = tg.download(url, checksum, { mode: "extract" }).then(tg.Directory.expect);
				return { PATH: tg.Mutation.suffix(tg`${dir}/bin`, ":") };
			};

			export default env;
		';
		$source | save ($path | path join 'tangram.ts')
		if ($config.authentication?.users?.providers?.insecure? | default false) {
			let user = tg -c ($config_path) login --verbose busyboxer | from json
			tg -c ($config_path) --token ($user.token) tag 'busybox' $path
			tg -c ($config_path) --token ($user.token) grant public tag_read 'busybox'
		} else {
			tg check $path
			tg -c ($config_path) tag 'busybox' $path
		}
		rm -rf $path
	}

	{ config: $config_path, directory: $directory_path, url: $url }
}

def clean_databases [id: string] {
	# Drop the Cockroach database.
	try { cockroach sql --insecure --host=localhost:26257 -e $'drop database if exists database_($id) cascade' }

	# Clear the fdb key range.
	let cluster = fdb_cluster
	try { ^timeout 10 fdbcli -C $cluster --exec $'writemode on; clearrange "index_($id)" "index_($id)\xff"; clearrange "logs_($id)" "logs_($id)\xff"' }

	# Drop the scylla keyspace.
	try { cqlsh -e $"drop keyspace \"objects_($id)\";" }
}

def diff [old: string, new: string, --path] {
	let old_path = if $path { $old } else { let t = mktemp; $old | save -f $t; $t }
	let new_path = if $path { $new } else { let t = mktemp; $new | save -f $t; $t }
	let result = delta --file-style=omit --hunk-header-style=omit --no-gitconfig $old_path $new_path | complete | get stdout
	if not $path { rm $old_path $new_path }
	$result
}

def literal [value: string, indent: string] {
	let raw = $value | str contains "'"
	let open = if $raw { "r#'" } else { "'" }
	let close = if $raw { "'#" } else { "'" }
	if ($value | str contains "\n") {
		let has_trailing_newline = $value | str ends-with "\n"
		let trimmed = $value | str trim --right --char "\n"
		let indented = $trimmed | split row "\n" | each { |line| $"($indent)\t($line)" } | str join "\n"
		if $has_trailing_newline {
			$"($open)\n($indented)\n\n($indent)($close)"
		} else {
			$"($open)\n($indented)\n($indent)($close)"
		}
	} else {
		$"($open)($value)($close)"
	}
}

def get_indent [source: string, position: int] {
	let before = $source | str substring ..<$position
	let line_start = $before | str index-of "\n" --end
	let line_start = if $line_start == -1 { 0 } else { $line_start + 1 }
	let line_prefix = $source | str substring $line_start..<$position
	$line_prefix | parse --regex '^(\s*)' | get 0.capture0? | default ''
}

export def --env success [
	output: record
	message?: string
] {
	if $output.exit_code != 0 {
		error make {
			msg: ($message | default 'the process failed'),
			label: {
				span: (metadata $output).span,
				text: 'the output',
			},
			help: $output.stderr,
		}
	}
}

# Skip the test, reporting the reason. The runner treats exit code 77 as skipped rather than passed or failed. This is named skip_test because skip is a Nushell builtin.
export def skip_test [reason: string] {
	print --stderr $reason
	exit 77
}

# Skip the test when the runner was invoked with --offline. Call this at the top of tests which require network access.
export def skip_if_offline [] {
	if (($env.TANGRAM_TEST_OFFLINE? | default '') | str length) > 0 {
		skip_test 'this test requires network access'
	}
}

# Determine whether the running kernel has enabled the FUSE io_uring transport.
export def fuse_io_uring_available [] {
	if $nu.os-info.name != 'linux' {
		return false
	}
	let path = '/sys/module/fuse/parameters/enable_uring'
	if not ($path | path exists) {
		return false
	}
	try {
		(open --raw $path | str trim | str lowercase) in ['1' 'y']
	} catch {
		false
	}
}

# Poll a condition until it returns true, erroring if the timeout elapses. Prefer this over a bare sleep, so the test runs as soon as the condition holds and tolerates slow machines.
export def wait_until [
	condition: closure
	message?: string
	--timeout: duration = 10sec
	--interval: duration = 50ms
] {
	let start = date now
	loop {
		if (do $condition) {
			return
		}
		if ((date now) - $start) > $timeout {
			error make {
				msg: ($message | default 'the condition was not met within the timeout'),
				label: {
					span: (metadata $condition).span,
					text: 'the condition',
				},
			}
		}
		sleep $interval
	}
}

# Redact literal strings in the input for snapshotting.
export def redact [...redactions: string] {
	mut output = $in
	for redaction in ($redactions | sort-by { |redaction| $redaction | str length } --reverse) {
		$output = $output | str replace --all $redaction '<redacted>'
	}
	$output
}

export def --env failure [
	output: record
	message?: string
] {
	if $output.exit_code == 0 {
		error make {
			msg: ($message | default 'the process succeeded'),
			label: {
				span: (metadata $output).span,
				text: 'the output',
			},
			help: $output.stderr,
		}
	}
}

export def xattr_list [path: string] {
	match $nu.os-info.name {
		'macos' => { xattr $path | lines }
		'linux' => { getfattr -m '.' $path | complete | get stdout | lines | where { |l| not ($l starts-with '#') and $l != '' } }
	}
}

export def xattr_read [name: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -p $name $path | str trim }
		'linux' => { getfattr -n $name --only-values $path | str trim }
	}
}

export def xattr_write [name: string, value: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -w $name $value $path }
		'linux' => { setfattr -n $name -v $value $path }
	}
}

# Normalize runtime IDs and tokens in a string for snapshotting. With --normalize-ids, include content-addressed IDs. The length floor keeps identifiers such as `pcs_id` from being normalized.
export def normalize [value?: string, --normalize-ids] {
	let input = $in
	let value = ($value | default $input)

	mut output = $value
	mut numeric_process_index = 0
	for id in ($output | parse --regex 'id = (?<id>[0-9]+)' | get id | uniq) {
		if $numeric_process_index > 9 {
			error make { msg: 'too many IDs to normalize for the prefix' }
		}
		let digit = $numeric_process_index | into string
		let replacement = 'pcs_00' + (0..<26 | each { $digit } | str join)
		$numeric_process_index += 1
		$output = $output | str replace --all $'id = ($id)' $'id = ($replacement)'
	}

	let prefixes = if $normalize_ids {
		[blb cmd dir err fil gph grp org pcs sbx sym usr]
	} else {
		[err grp org pcs sbx usr]
	}
	let prefixes_pattern = $prefixes | str join '|'
	let pattern = '(?<id>(' + $prefixes_pattern + ')_[a-z0-9]{20,})'
	mut counters = {}
	for id in ($output | parse --regex $pattern | get id | uniq) {
		let prefix = ($id | split row '_' | first)
		let suffix = ($id | split row '_' | last)
		let index = ($counters | get --optional $prefix | default 0)
		if $index > 9 {
			error make { msg: 'too many IDs to normalize for the prefix' }
		}
		let header_length = if ($suffix | str length) < 2 { $suffix | str length } else { 2 }
		let header = ($suffix | str substring 0..<$header_length)
		let digit = ($index | into string)
		let replacement_length = ($suffix | str length) - $header_length
		let replacement_suffix = if $replacement_length <= 0 {
			''
		} else {
			0..<$replacement_length | each { $digit } | str join
		}
		let replacement = $'($prefix)_($header)($replacement_suffix)'
		$counters = ($counters | upsert $prefix ($index + 1))
		$output = $output | str replace --all $id $replacement
	}

	$output = $output | str replace --all --regex '([?&]token=|"token":\s*")[A-Za-z0-9._~%+/=-]+' '${1}<token>'

	$output
}

def server_exit_path [temp_path: string, job_id: int] {
	$temp_path | path join $server_exit_directory_name | path join $'($job_id).exit'
}

def count_tangram_processes [] {
	tangram_process_pids_list | length
}

def tangram_process_pids_list [] {
	let output = (^bash -c (tangram_process_pids) | complete)
	if $output.exit_code != 0 {
		return []
	}
	$output.stdout | lines | str trim | where { not ($in | is-empty) }
}

def lmdb_sysv_keys_for_test_dirs [paths: list] {
	let lockfiles = $paths | each { |path|
		[
			(glob ($path | path join '**/index-lock')),
			(glob ($path | path join '**/logs-lock')),
			(glob ($path | path join '**/objects-lock')),
		] | flatten
	} | flatten | uniq
	if ($lockfiles | is-empty) {
		return []
	}
	let output = (
		^/usr/bin/perl -MIPC::SysV=ftok -e 'for my $path (@ARGV) { my $key = ftok($path, ord("M")); printf "0x%08x\n", $key if defined($key) && $key != -1; }' ...$lockfiles | complete
	)
	if $output.exit_code != 0 {
		return []
	}
	$output.stdout | lines | where { not ($in | is-empty) } | uniq
}

def count_lmdb_sysv_semaphores [keys: list] {
	if ($keys | is-empty) {
		return 0
	}
	let output = (^ipcs -s | complete)
	if $output.exit_code != 0 {
		return 0
	}
	let user = $env.USER? | default ''
	$output.stdout | lines | skip 3 | where { |line|
		let columns = $line | split row --regex '\s+' | where { $in != '' }
		($columns | length) >= 5 and ($columns | get 2) in $keys and (($user | is-empty) or (($columns | get 4) == $user))
	} | length
}

def process_supervisor [] {
	'
	set -m
	parent_pid=$1
	shift

	"$@" &
	child=$!

	child_done() {
		stat=$(ps -o stat= -p "$child" 2>/dev/null | tr -d " ")
		case "$stat" in
			""|Z*) return 0 ;;
			*) return 1 ;;
		esac
	}

	terminate_child() {
		kill -TERM -- -"$child" 2>/dev/null || true
		kill -TERM "$child" 2>/dev/null || true
		for _ in $(seq 1 100); do
			if child_done; then
				return
			fi
			sleep 0.05
		done
		kill -KILL "$child" 2>/dev/null || true
	}

	(
		while kill -0 "$parent_pid" 2>/dev/null && ! child_done; do
			sleep 0.05
		done
		if ! child_done; then
			terminate_child
		fi
	) &
	watcher=$!

	trap "terminate_child" TERM INT HUP

	wait "$child"
	status=$?

	trap - TERM INT HUP
	kill "$watcher" 2>/dev/null || true
	wait "$watcher" 2>/dev/null || true
	exit "$status"
'
}

def fdb_cluster [] {
	let env_cluster = $env.TANGRAM_TEST_FDB_CLUSTER? | default ''
	if ($env_cluster | str length) > 0 {
		return $env_cluster
	}

	let system_cluster = '/etc/foundationdb/fdb.cluster'
	if ($system_cluster | path exists) {
		return $system_cluster
	}

	let user_cluster = ($nu.home-dir | path join '.foundationdb/fdb.cluster')
	if ($user_cluster | path exists) {
		return $user_cluster
	}

	let cluster = mktemp -t
	"local:local@localhost:4500" | save -f $cluster
	$cluster
}

def clean_tangram_processes [] {
	let pids = tangram_process_pids_list
	if ($pids | is-empty) {
		return
	}
	for pid in $pids {
		try { ^bash -c 'kill -TERM -- -"$1" 2>/dev/null || true; kill -TERM "$1" 2>/dev/null || true' _ $pid }
	}
	for _ in 1..100 {
		let remaining = $pids | where { |pid|
			(^bash -c 'kill -0 "$1" 2>/dev/null' _ $pid | complete).exit_code == 0
		}
		if ($remaining | is-empty) {
			return
		}
		sleep 50ms
	}
	for pid in $pids {
		try { ^bash -c 'kill -KILL -- -"$1" 2>/dev/null || true; kill -KILL "$1" 2>/dev/null || true' _ $pid }
	}
}

def tangram_process_pids [] {
	'
		ps -axo pid=,command= | while read -r pid command; do
			if [ -z "$pid" ] || [ -z "$command" ]; then
				continue
			fi
			executable=${command%% *}
			case "$(basename "$executable" 2>/dev/null)" in
				tangram|tg) ;;
				*) continue ;;
			esac
			path=$(realpath "$executable" 2>/dev/null || true)
			if [ "$(basename "$path" 2>/dev/null)" = "tangram" ]; then
				printf "%s\n" "$pid"
			fi
		done
	'
}

def clean_lmdb_sysv_semaphores [keys: list] {
	if ($keys | is-empty) {
		return
	}
	let user = $env.USER? | default ''
	let output = (^ipcs -s | complete)
	if $output.exit_code != 0 {
		return
	}
	let semaphore_ids = $output.stdout | lines | skip 3 | where { |line|
		let columns = $line | split row --regex '\s+' | where { $in != '' }
		($columns | length) >= 5 and ($columns | get 2) in $keys and (($user | is-empty) or (($columns | get 4) == $user))
	} | each { |line|
		$line | split row --regex '\s+' | where { $in != '' } | get 1
	}
	for id in $semaphore_ids {
		try { ^ipcrm -s $id }
	}
}

export def cleanup_background_jobs [temp_path: string] {
	# Kill any background jobs started by the test, such as server and LSP processes.
	for job in (job list | where { ($in.description? | default '') == 'lsp' }) {
		for pid in ($job.pids? | default []) {
			try { ^bash -c 'kill -KILL -- -"$1" 2>/dev/null || true; kill -KILL "$1" 2>/dev/null || true' _ $pid }
		}
		try { job kill $job.id }
	}

	for job in (job list | where { ($in.description? | default '') == 'server' } | sort-by id | reverse) {
		let exit_path = server_exit_path $temp_path $job.id
		stop_server_job $job.id
		if not (wait_for_server_exit $exit_path) {
			try { job kill $job.id }
		}
	}
}

def stop_server_job [job_id: int] {
	for job in (job list | where id == $job_id) {
		for pid in ($job.pids? | default []) {
			try { ^bash -c 'children=$(pgrep -P "$1" 2>/dev/null || true); kill -TERM "$1" 2>/dev/null || true; for child in $children; do kill -TERM "$child" 2>/dev/null || true; done' _ $pid }
		}
	}
}

def wait_for_server_exit [path: string] {
	if ($path | path exists) {
		return true
	}
	let output = (open /dev/null | timeout 5 bash -c 'while [ ! -e "$1" ]; do sleep 0.05; done' _ $path | complete)
	$output.exit_code == 0 or ($path | path exists)
}

def remove_temp_directory [path: string] {
	if not ($path | path exists) {
		return
	}
	force_unmount_vfs $path
	try { chmod -R u+rwx $path }
	try { rm -rf $path }
}

def force_unmount_vfs [path: string] {
	match $nu.os-info.name {
		'linux' => { force_unmount_vfs_linux $path },
		'macos' => { force_unmount_vfs_macos $path },
		_ => {},
	}
}

# Unmounts any fskit vfs left under the path. A server that exits cleanly unmounts itself, so this only catches the ones that crashed.
def force_unmount_vfs_macos [path: string] {
	let targets = (
		try {
			^mount | lines | each { |line|
				let matches = ($line | parse --regex '^.+ on (?<target>.+) \(tangram[,)]')
				if ($matches | is-empty) { null } else { $matches | first | get target }
			} | compact
		} catch {
			[]
		}
	)
	let artifacts_paths = (
		$targets
		| where { |target| ($target == ($path | path join 'artifacts')) or ($target | str starts-with ($path + '/')) }
		| uniq
		| each { |path| { path: $path, length: ($path | str length) } }
		| sort-by length --reverse
		| get path
	)
	for artifacts_path in $artifacts_paths {
		try { ^umount -f $artifacts_path o> /dev/null e> /dev/null }
	}
}

def force_unmount_vfs_linux [path: string] {
	let mounted_artifacts_paths = (
		try {
			^findmnt -rn -o TARGET | lines | where { |target|
				($target == ($path | path join 'artifacts')) or (($target | str starts-with ($path + '/')) and (($target | path basename) == 'artifacts'))
			}
		} catch {
			[]
		}
	)
	let artifacts_paths = (
		[
			($path | path join 'artifacts')
		] | append (
			try {
				^fd -a -t d '^artifacts$' $path | lines
			} catch {
				[]
			}
		) | append $mounted_artifacts_paths | uniq | each { |path|
			{ path: $path, length: ($path | str length) }
		} | sort-by length --reverse | get path
	)
	for artifacts_path in $artifacts_paths {
		try {
			^fusermount3 -u -z $artifacts_path o> /dev/null e> /dev/null
		}
	}
}
